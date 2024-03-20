// Modified from <https://github.com/denoland/deno/blob/main/runtime/permissions/mod.rs>
// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.

use anyhow::Context;
use deno_core::error::custom_error;
use deno_core::error::uri_error;
use deno_core::error::AnyError;
use deno_core::normalize_path;
use deno_core::parking_lot::Mutex;
use deno_core::serde::de;
use deno_core::serde::Deserialize;
use deno_core::serde::Deserializer;
use deno_core::serde::Serialize;
use deno_core::serde_json;
use deno_core::url;
use deno_core::ModuleSpecifier;
use std::borrow::Cow;
use std::collections::HashSet;
use std::env::current_dir;
use std::fmt;
use std::hash::Hash;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::string::ToString;
use std::sync::Arc;

#[allow(dead_code)]
pub const PERMISSION_EMOJI: &str = "⚠️";

/// Quadri-state value for storing permission state
#[derive(Eq, PartialEq, Default, Debug, Clone, Copy, Deserialize, PartialOrd)]
pub enum PermissionState {
    Granted = 0,
    GrantedPartial = 1,
    #[default]
    Prompt = 2,
    Denied = 3,
}

/// `AllowPartial` prescribes how to treat a permission which is partially
/// denied due to a `--deny-*` flag affecting a subscope of the queried
/// permission.
///
/// `TreatAsGranted` is used in place of `TreatAsPartialGranted` when we don't
/// want to wastefully check for partial denials when, say, checking read
/// access for a file.
#[derive(Debug, Eq, PartialEq)]
#[allow(clippy::enum_variant_names)]
enum AllowPartial {
    TreatAsGranted,
    TreatAsDenied,
    TreatAsPartialGranted,
}

impl From<bool> for AllowPartial {
    fn from(value: bool) -> Self {
        if value {
            Self::TreatAsGranted
        } else {
            Self::TreatAsDenied
        }
    }
}

impl PermissionState {
    #[inline(always)]
    fn log_perm_access(_name: &str, _info: impl FnOnce() -> Option<String>) {
        // Disabled it for now
    }

    fn fmt_access(name: &str, info: impl FnOnce() -> Option<String>) -> String {
        format!(
            "{} access{}",
            name,
            info()
                .map(|info| { format!(" to {info}") })
                .unwrap_or_default(),
        )
    }

    fn error(name: &str, info: impl FnOnce() -> Option<String>) -> AnyError {
        custom_error(
            "PermissionDenied",
            format!(
                "Requires {}, run again with the --allow-{} flag",
                Self::fmt_access(name, info),
                name
            ),
        )
    }

    /// Check the permission state. bool is whether a prompt was issued.
    #[inline]
    fn check(
        self,
        name: &str,
        api_name: Option<&str>,
        info: Option<&str>,
        prompt: bool,
    ) -> (Result<(), AnyError>, bool, bool) {
        self.check2(name, api_name, || info.map(|s| s.to_string()), prompt)
    }

    #[inline]
    fn check2(
        self,
        name: &str,
        _api_name: Option<&str>,
        info: impl Fn() -> Option<String>,
        _prompt: bool,
    ) -> (Result<(), AnyError>, bool, bool) {
        match self {
            PermissionState::Granted => {
                Self::log_perm_access(name, info);
                (Ok(()), false, false)
            }
            _ => (Err(Self::error(name, info)), false, false),
        }
    }
}

impl fmt::Display for PermissionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PermissionState::Granted => f.pad("granted"),
            PermissionState::GrantedPartial => f.pad("granted-partial"),
            PermissionState::Prompt => f.pad("prompt"),
            PermissionState::Denied => f.pad("denied"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnitPermission {
    pub name: &'static str,
    pub description: &'static str,
    pub state: PermissionState,
    pub prompt: bool,
}

impl UnitPermission {
    pub fn query(&self) -> PermissionState {
        self.state
    }

    pub fn request(&mut self) -> PermissionState {
        if self.state == PermissionState::Prompt {
            self.state = PermissionState::Denied;
        }
        self.state
    }

    pub fn revoke(&mut self) -> PermissionState {
        if self.state == PermissionState::Granted {
            self.state = PermissionState::Prompt;
        }
        self.state
    }

    pub fn check(&mut self) -> Result<(), AnyError> {
        let (result, prompted, _is_allow_all) =
            self.state.check(self.name, None, None, self.prompt);
        if prompted {
            if result.is_ok() {
                self.state = PermissionState::Granted;
            } else {
                self.state = PermissionState::Denied;
            }
        }
        result
    }
}

/// A normalized environment variable name. On Windows this will
/// be uppercase and on other platforms it will stay as-is.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
struct EnvVarName {
    inner: String,
}

impl EnvVarName {
    pub fn new(env: impl AsRef<str>) -> Self {
        Self {
            inner: if cfg!(windows) {
                env.as_ref().to_uppercase()
            } else {
                env.as_ref().to_string()
            },
        }
    }
}

impl AsRef<str> for EnvVarName {
    fn as_ref(&self) -> &str {
        self.inner.as_str()
    }
}

pub trait Descriptor: Eq + Clone {
    fn flag_name() -> &'static str;
    fn name(&self) -> Cow<str>;
    // By default, specifies no-stronger-than relationship.
    // As this is not strict, it's only true when descriptors are the same.
    fn stronger_than(&self, other: &Self) -> bool {
        self == other
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnaryPermission<T: Descriptor + Hash> {
    pub granted_global: bool,
    pub granted_list: HashSet<T>,
    pub flag_denied_global: bool,
    pub flag_denied_list: HashSet<T>,
    pub prompt_denied_global: bool,
    pub prompt_denied_list: HashSet<T>,
    pub prompt: bool,
}

impl<T: Descriptor + Hash> Default for UnaryPermission<T> {
    fn default() -> Self {
        UnaryPermission {
            granted_global: Default::default(),
            granted_list: Default::default(),
            flag_denied_global: Default::default(),
            flag_denied_list: Default::default(),
            prompt_denied_global: Default::default(),
            prompt_denied_list: Default::default(),
            prompt: Default::default(),
        }
    }
}

impl<T: Descriptor + Hash> UnaryPermission<T> {
    fn check_desc(
        &mut self,
        desc: &Option<T>,
        assert_non_partial: bool,
        api_name: Option<&str>,
        get_display_name: impl Fn() -> Option<String>,
    ) -> Result<(), AnyError> {
        let (result, prompted, is_allow_all) = self
            .query_desc(desc, AllowPartial::from(assert_non_partial))
            .check2(
                T::flag_name(),
                api_name,
                || match get_display_name() {
                    Some(display_name) => Some(display_name),
                    None => desc.as_ref().map(|d| format!("\"{}\"", d.name())),
                },
                self.prompt,
            );
        if prompted {
            if result.is_ok() {
                if is_allow_all {
                    self.insert_granted(None);
                } else {
                    self.insert_granted(desc.clone());
                }
            } else {
                self.insert_prompt_denied(desc.clone());
            }
        }
        result
    }

    fn query_desc(&self, desc: &Option<T>, allow_partial: AllowPartial) -> PermissionState {
        if self.is_flag_denied(desc) || self.is_prompt_denied(desc) {
            PermissionState::Denied
        } else if self.is_granted(desc) {
            match allow_partial {
                AllowPartial::TreatAsGranted => PermissionState::Granted,
                AllowPartial::TreatAsDenied => {
                    if self.is_partial_flag_denied(desc) {
                        PermissionState::Denied
                    } else {
                        PermissionState::Granted
                    }
                }
                AllowPartial::TreatAsPartialGranted => {
                    if self.is_partial_flag_denied(desc) {
                        PermissionState::GrantedPartial
                    } else {
                        PermissionState::Granted
                    }
                }
            }
        } else if matches!(allow_partial, AllowPartial::TreatAsDenied)
            && self.is_partial_flag_denied(desc)
        {
            PermissionState::Denied
        } else {
            PermissionState::Prompt
        }
    }

    fn request_desc(
        &mut self,
        desc: &Option<T>,
        get_display_name: impl Fn() -> Option<String>,
    ) -> PermissionState {
        let state = self.query_desc(desc, AllowPartial::TreatAsPartialGranted);
        if state == PermissionState::Granted {
            self.insert_granted(desc.clone());
            return state;
        }
        if state != PermissionState::Prompt {
            return state;
        }
        let mut message = String::with_capacity(40);
        message.push_str(&format!("{} access", T::flag_name()));
        match get_display_name() {
            Some(display_name) => message.push_str(&format!(" to \"{}\"", display_name)),
            None => match desc {
                Some(desc) => message.push_str(&format!(" to \"{}\"", desc.name())),
                None => {}
            },
        }
        PermissionState::Denied
    }

    fn revoke_desc(&mut self, desc: &Option<T>) -> PermissionState {
        match desc.as_ref() {
            Some(desc) => self.granted_list.retain(|v| !v.stronger_than(desc)),
            None => {
                self.granted_global = false;
                // Revoke global is a special case where the entire granted list is
                // cleared. It's inconsistent with the granular case where only
                // descriptors stronger than the revoked one are purged.
                self.granted_list.clear();
            }
        }
        self.query_desc(desc, AllowPartial::TreatAsPartialGranted)
    }

    fn is_granted(&self, desc: &Option<T>) -> bool {
        Self::list_contains(desc, self.granted_global, &self.granted_list)
    }

    fn is_flag_denied(&self, desc: &Option<T>) -> bool {
        Self::list_contains(desc, self.flag_denied_global, &self.flag_denied_list)
    }

    fn is_prompt_denied(&self, desc: &Option<T>) -> bool {
        match desc.as_ref() {
            Some(desc) => self
                .prompt_denied_list
                .iter()
                .any(|v| desc.stronger_than(v)),
            None => self.prompt_denied_global || !self.prompt_denied_list.is_empty(),
        }
    }

    fn is_partial_flag_denied(&self, desc: &Option<T>) -> bool {
        match desc {
            None => !self.flag_denied_list.is_empty(),
            Some(desc) => self.flag_denied_list.iter().any(|v| desc.stronger_than(v)),
        }
    }

    fn list_contains(desc: &Option<T>, list_global: bool, list: &HashSet<T>) -> bool {
        match desc.as_ref() {
            Some(desc) => list_global || list.iter().any(|v| v.stronger_than(desc)),
            None => list_global,
        }
    }

    fn insert_granted(&mut self, desc: Option<T>) {
        Self::list_insert(desc, &mut self.granted_global, &mut self.granted_list);
    }

    fn insert_prompt_denied(&mut self, desc: Option<T>) {
        Self::list_insert(
            desc,
            &mut self.prompt_denied_global,
            &mut self.prompt_denied_list,
        );
    }

    fn list_insert(desc: Option<T>, list_global: &mut bool, list: &mut HashSet<T>) {
        match desc {
            Some(desc) => {
                list.insert(desc);
            }
            None => *list_global = true,
        }
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct ReadDescriptor(pub PathBuf);

impl Descriptor for ReadDescriptor {
    fn flag_name() -> &'static str {
        "read"
    }

    fn name(&self) -> Cow<str> {
        Cow::from(self.0.display().to_string())
    }

    fn stronger_than(&self, other: &Self) -> bool {
        other.0.starts_with(&self.0)
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct WriteDescriptor(pub PathBuf);

impl Descriptor for WriteDescriptor {
    fn flag_name() -> &'static str {
        "write"
    }

    fn name(&self) -> Cow<str> {
        Cow::from(self.0.display().to_string())
    }

    fn stronger_than(&self, other: &Self) -> bool {
        other.0.starts_with(&self.0)
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct NetDescriptor(pub String, pub Option<u16>);

impl NetDescriptor {
    fn new<T: AsRef<str>>(host: &&(T, Option<u16>)) -> Self {
        NetDescriptor(host.0.as_ref().to_string(), host.1)
    }
}

impl Descriptor for NetDescriptor {
    fn flag_name() -> &'static str {
        "net"
    }

    fn name(&self) -> Cow<str> {
        Cow::from(format!("{}", self))
    }

    fn stronger_than(&self, other: &Self) -> bool {
        self.0 == other.0 && (self.1.is_none() || self.1 == other.1)
    }
}

impl FromStr for NetDescriptor {
    type Err = AnyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let url = url::Url::parse(&format!("http://{s}"))?;
        let hostname = url.host_str().unwrap().to_string();

        Ok(NetDescriptor(hostname, url.port()))
    }
}

impl fmt::Display for NetDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&match self.1 {
            None => self.0.clone(),
            Some(port) => format!("{}:{}", self.0, port),
        })
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct EnvDescriptor(EnvVarName);

impl EnvDescriptor {
    pub fn new(env: impl AsRef<str>) -> Self {
        Self(EnvVarName::new(env))
    }
}

impl Descriptor for EnvDescriptor {
    fn flag_name() -> &'static str {
        "env"
    }

    fn name(&self) -> Cow<str> {
        Cow::from(self.0.as_ref())
    }
}

impl AsRef<str> for EnvDescriptor {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum RunDescriptor {
    Name(String),
    Path(PathBuf),
}

impl Descriptor for RunDescriptor {
    fn flag_name() -> &'static str {
        "run"
    }

    fn name(&self) -> Cow<str> {
        Cow::from(self.to_string())
    }
}

impl FromStr for RunDescriptor {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let is_path = s.contains('/');
        #[cfg(windows)]
        let is_path = is_path || s.contains('\\') || Path::new(s).is_absolute();
        if is_path {
            Ok(Self::Path(resolve_from_cwd(Path::new(s)).unwrap()))
        } else {
            Ok(Self::Name(s.to_string()))
        }
    }
}

impl ToString for RunDescriptor {
    fn to_string(&self) -> String {
        match self {
            RunDescriptor::Name(s) => s.clone(),
            RunDescriptor::Path(p) => p.to_string_lossy().to_string(),
        }
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct SysDescriptor(pub String);

impl Descriptor for SysDescriptor {
    fn flag_name() -> &'static str {
        "sys"
    }

    fn name(&self) -> Cow<str> {
        Cow::from(self.0.to_string())
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct FfiDescriptor(pub PathBuf);

impl Descriptor for FfiDescriptor {
    fn flag_name() -> &'static str {
        "ffi"
    }

    fn name(&self) -> Cow<str> {
        Cow::from(self.0.display().to_string())
    }

    fn stronger_than(&self, other: &Self) -> bool {
        other.0.starts_with(&self.0)
    }
}

impl UnaryPermission<ReadDescriptor> {
    pub fn query(&self, path: Option<&Path>) -> PermissionState {
        self.query_desc(
            &path.map(|p| ReadDescriptor(resolve_from_cwd(p).unwrap())),
            AllowPartial::TreatAsPartialGranted,
        )
    }

    pub fn request(&mut self, path: Option<&Path>) -> PermissionState {
        self.request_desc(
            &path.map(|p| ReadDescriptor(resolve_from_cwd(p).unwrap())),
            || Some(path?.display().to_string()),
        )
    }

    pub fn revoke(&mut self, path: Option<&Path>) -> PermissionState {
        self.revoke_desc(&path.map(|p| ReadDescriptor(resolve_from_cwd(p).unwrap())))
    }

    pub fn check(&mut self, path: &Path, api_name: Option<&str>) -> Result<(), AnyError> {
        self.check_desc(
            &Some(ReadDescriptor(resolve_from_cwd(path)?)),
            true,
            api_name,
            || Some(format!("\"{}\"", path.display())),
        )
    }

    #[inline]
    #[allow(dead_code)]
    pub fn check_partial(&mut self, path: &Path, api_name: Option<&str>) -> Result<(), AnyError> {
        let desc = ReadDescriptor(resolve_from_cwd(path)?);
        self.check_desc(&Some(desc), false, api_name, || {
            Some(format!("\"{}\"", path.display()))
        })
    }

    /// As `check()`, but permission error messages will anonymize the path
    /// by replacing it with the given `display`.
    pub fn check_blind(
        &mut self,
        path: &Path,
        display: &str,
        api_name: &str,
    ) -> Result<(), AnyError> {
        let desc = ReadDescriptor(resolve_from_cwd(path)?);
        self.check_desc(&Some(desc), false, Some(api_name), || {
            Some(format!("<{display}>"))
        })
    }

    pub fn check_all(&mut self, api_name: Option<&str>) -> Result<(), AnyError> {
        self.check_desc(&None, false, api_name, || None)
    }
}

impl UnaryPermission<WriteDescriptor> {
    pub fn query(&self, path: Option<&Path>) -> PermissionState {
        self.query_desc(
            &path.map(|p| WriteDescriptor(resolve_from_cwd(p).unwrap())),
            AllowPartial::TreatAsPartialGranted,
        )
    }

    pub fn request(&mut self, path: Option<&Path>) -> PermissionState {
        self.request_desc(
            &path.map(|p| WriteDescriptor(resolve_from_cwd(p).unwrap())),
            || Some(path?.display().to_string()),
        )
    }

    pub fn revoke(&mut self, path: Option<&Path>) -> PermissionState {
        self.revoke_desc(&path.map(|p| WriteDescriptor(resolve_from_cwd(p).unwrap())))
    }

    pub fn check(&mut self, path: &Path, api_name: Option<&str>) -> Result<(), AnyError> {
        self.check_desc(
            &Some(WriteDescriptor(resolve_from_cwd(path)?)),
            true,
            api_name,
            || Some(format!("\"{}\"", path.display())),
        )
    }

    /// As `check()`, but permission error messages will anonymize the path
    /// by replacing it with the given `display`.
    pub fn check_blind(
        &mut self,
        path: &Path,
        display: &str,
        api_name: &str,
    ) -> Result<(), AnyError> {
        let desc = WriteDescriptor(resolve_from_cwd(path)?);
        self.check_desc(&Some(desc), false, Some(api_name), || {
            Some(format!("<{display}>"))
        })
    }

    pub fn check_all(&mut self, api_name: Option<&str>) -> Result<(), AnyError> {
        self.check_desc(&None, false, api_name, || None)
    }
}

impl UnaryPermission<NetDescriptor> {
    pub fn query<T: AsRef<str>>(&self, host: Option<&(T, Option<u16>)>) -> PermissionState {
        self.query_desc(
            &host.map(|h| NetDescriptor::new(&h)),
            AllowPartial::TreatAsPartialGranted,
        )
    }

    pub fn request<T: AsRef<str>>(&mut self, host: Option<&(T, Option<u16>)>) -> PermissionState {
        self.request_desc(&host.map(|h| NetDescriptor::new(&h)), || None)
    }

    pub fn revoke<T: AsRef<str>>(&mut self, host: Option<&(T, Option<u16>)>) -> PermissionState {
        self.revoke_desc(&Some(NetDescriptor::new(&host.unwrap())))
    }

    pub fn check<T: AsRef<str>>(
        &mut self,
        host: &(T, Option<u16>),
        api_name: Option<&str>,
    ) -> Result<(), AnyError> {
        self.check_desc(&Some(NetDescriptor::new(&host)), false, api_name, || None)
    }

    pub fn check_url(&mut self, url: &url::Url, api_name: Option<&str>) -> Result<(), AnyError> {
        let hostname = url
            .host_str()
            .ok_or_else(|| uri_error("Missing host"))?
            .to_string();
        let host = &(&hostname, url.port_or_known_default());
        let display_host = match url.port() {
            None => hostname.clone(),
            Some(port) => format!("{hostname}:{port}"),
        };
        self.check_desc(&Some(NetDescriptor::new(&host)), false, api_name, || {
            Some(format!("\"{}\"", display_host))
        })
    }
    #[allow(dead_code)]
    pub fn check_all(&mut self) -> Result<(), AnyError> {
        self.check_desc(&None, false, None, || None)
    }
}

impl UnaryPermission<EnvDescriptor> {
    pub fn query(&self, env: Option<&str>) -> PermissionState {
        self.query_desc(
            &env.map(EnvDescriptor::new),
            AllowPartial::TreatAsPartialGranted,
        )
    }

    pub fn request(&mut self, env: Option<&str>) -> PermissionState {
        self.request_desc(&env.map(EnvDescriptor::new), || None)
    }

    pub fn revoke(&mut self, env: Option<&str>) -> PermissionState {
        self.revoke_desc(&env.map(EnvDescriptor::new))
    }

    pub fn check(&mut self, env: &str) -> Result<(), AnyError> {
        self.check_desc(&Some(EnvDescriptor::new(env)), false, None, || None)
    }

    pub fn check_all(&mut self) -> Result<(), AnyError> {
        self.check_desc(&None, false, None, || None)
    }
}

impl UnaryPermission<SysDescriptor> {
    pub fn query(&self, kind: Option<&str>) -> PermissionState {
        self.query_desc(
            &kind.map(|k| SysDescriptor(k.to_string())),
            AllowPartial::TreatAsPartialGranted,
        )
    }

    pub fn request(&mut self, kind: Option<&str>) -> PermissionState {
        self.request_desc(&kind.map(|k| SysDescriptor(k.to_string())), || None)
    }

    pub fn revoke(&mut self, kind: Option<&str>) -> PermissionState {
        self.revoke_desc(&kind.map(|k| SysDescriptor(k.to_string())))
    }

    pub fn check(&mut self, kind: &str, api_name: Option<&str>) -> Result<(), AnyError> {
        self.check_desc(
            &Some(SysDescriptor(kind.to_string())),
            false,
            api_name,
            || None,
        )
    }

    #[allow(dead_code)]
    pub fn check_all(&mut self) -> Result<(), AnyError> {
        self.check_desc(&None, false, None, || None)
    }
}

impl UnaryPermission<RunDescriptor> {
    pub fn query(&self, cmd: Option<&str>) -> PermissionState {
        self.query_desc(
            &cmd.map(|c| RunDescriptor::from_str(c).unwrap()),
            AllowPartial::TreatAsPartialGranted,
        )
    }

    pub fn request(&mut self, cmd: Option<&str>) -> PermissionState {
        self.request_desc(&cmd.map(|c| RunDescriptor::from_str(c).unwrap()), || {
            Some(cmd?.to_string())
        })
    }

    pub fn revoke(&mut self, cmd: Option<&str>) -> PermissionState {
        self.revoke_desc(&cmd.map(|c| RunDescriptor::from_str(c).unwrap()))
    }

    pub fn check(&mut self, cmd: &str, api_name: Option<&str>) -> Result<(), AnyError> {
        self.check_desc(
            &Some(RunDescriptor::from_str(cmd).unwrap()),
            false,
            api_name,
            || Some(format!("\"{}\"", cmd)),
        )
    }

    pub fn check_all(&mut self, api_name: Option<&str>) -> Result<(), AnyError> {
        self.check_desc(&None, false, api_name, || None)
    }
}

impl UnaryPermission<FfiDescriptor> {
    pub fn query(&self, path: Option<&Path>) -> PermissionState {
        self.query_desc(
            &path.map(|p| FfiDescriptor(resolve_from_cwd(p).unwrap())),
            AllowPartial::TreatAsPartialGranted,
        )
    }

    pub fn request(&mut self, path: Option<&Path>) -> PermissionState {
        self.request_desc(
            &path.map(|p| FfiDescriptor(resolve_from_cwd(p).unwrap())),
            || Some(path?.display().to_string()),
        )
    }

    pub fn revoke(&mut self, path: Option<&Path>) -> PermissionState {
        self.revoke_desc(&path.map(|p| FfiDescriptor(resolve_from_cwd(p).unwrap())))
    }

    pub fn check(&mut self, path: &Path, api_name: Option<&str>) -> Result<(), AnyError> {
        self.check_desc(
            &Some(FfiDescriptor(resolve_from_cwd(path)?)),
            true,
            api_name,
            || Some(format!("\"{}\"", path.display())),
        )
    }

    #[allow(dead_code)]
    pub fn check_partial(&mut self, path: Option<&Path>) -> Result<(), AnyError> {
        let desc = match path {
            Some(path) => Some(FfiDescriptor(resolve_from_cwd(path)?)),
            None => None,
        };
        self.check_desc(&desc, false, None, || {
            Some(format!("\"{}\"", path?.display()))
        })
    }

    pub fn check_all(&mut self) -> Result<(), AnyError> {
        self.check_desc(&None, false, Some("all"), || None)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Permissions {
    pub read: UnaryPermission<ReadDescriptor>,
    pub write: UnaryPermission<WriteDescriptor>,
    pub net: UnaryPermission<NetDescriptor>,
    pub env: UnaryPermission<EnvDescriptor>,
    pub sys: UnaryPermission<SysDescriptor>,
    pub run: UnaryPermission<RunDescriptor>,
    pub ffi: UnaryPermission<FfiDescriptor>,
    pub hrtime: UnitPermission,
}

impl Default for Permissions {
    fn default() -> Self {
        Self {
            read: Permissions::new_read(&None, &None, false).unwrap(),
            write: Permissions::new_write(&None, &None, false).unwrap(),
            net: Permissions::new_net(&None, &None, false).unwrap(),
            env: Permissions::new_env(&None, &None, false).unwrap(),
            sys: Permissions::new_sys(&None, &None, false).unwrap(),
            run: Permissions::new_run(&None, &None, false).unwrap(),
            ffi: Permissions::new_ffi(&None, &None, false).unwrap(),
            hrtime: Permissions::new_hrtime(false, false),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Default, Serialize, Deserialize)]
pub struct PermissionsOptions {
    pub allow_env: Option<Vec<String>>,
    pub deny_env: Option<Vec<String>>,
    pub allow_hrtime: bool,
    pub deny_hrtime: bool,
    pub allow_net: Option<Vec<String>>,
    pub deny_net: Option<Vec<String>>,
    pub allow_ffi: Option<Vec<PathBuf>>,
    pub deny_ffi: Option<Vec<PathBuf>>,
    pub allow_read: Option<Vec<PathBuf>>,
    pub deny_read: Option<Vec<PathBuf>>,
    pub allow_run: Option<Vec<String>>,
    pub deny_run: Option<Vec<String>>,
    pub allow_sys: Option<Vec<String>>,
    pub deny_sys: Option<Vec<String>>,
    pub allow_write: Option<Vec<PathBuf>>,
    pub deny_write: Option<Vec<PathBuf>>,
    pub prompt: bool,
}

impl Permissions {
    pub fn new_read(
        allow_list: &Option<Vec<PathBuf>>,
        deny_list: &Option<Vec<PathBuf>>,
        prompt: bool,
    ) -> Result<UnaryPermission<ReadDescriptor>, AnyError> {
        Ok(UnaryPermission::<ReadDescriptor> {
            granted_global: global_from_option(allow_list),
            granted_list: parse_path_list(allow_list, ReadDescriptor)?,
            flag_denied_global: global_from_option(deny_list),
            flag_denied_list: parse_path_list(deny_list, ReadDescriptor)?,
            prompt,
            ..Default::default()
        })
    }

    pub fn new_write(
        allow_list: &Option<Vec<PathBuf>>,
        deny_list: &Option<Vec<PathBuf>>,
        prompt: bool,
    ) -> Result<UnaryPermission<WriteDescriptor>, AnyError> {
        Ok(UnaryPermission {
            granted_global: global_from_option(allow_list),
            granted_list: parse_path_list(allow_list, WriteDescriptor)?,
            flag_denied_global: global_from_option(deny_list),
            flag_denied_list: parse_path_list(deny_list, WriteDescriptor)?,
            prompt,
            ..Default::default()
        })
    }

    pub fn new_net(
        allow_list: &Option<Vec<String>>,
        deny_list: &Option<Vec<String>>,
        prompt: bool,
    ) -> Result<UnaryPermission<NetDescriptor>, AnyError> {
        Ok(UnaryPermission::<NetDescriptor> {
            granted_global: global_from_option(allow_list),
            granted_list: parse_net_list(allow_list)?,
            flag_denied_global: global_from_option(deny_list),
            flag_denied_list: parse_net_list(deny_list)?,
            prompt,
            ..Default::default()
        })
    }

    pub fn new_env(
        allow_list: &Option<Vec<String>>,
        deny_list: &Option<Vec<String>>,
        prompt: bool,
    ) -> Result<UnaryPermission<EnvDescriptor>, AnyError> {
        Ok(UnaryPermission::<EnvDescriptor> {
            granted_global: global_from_option(allow_list),
            granted_list: parse_env_list(allow_list)?,
            flag_denied_global: global_from_option(deny_list),
            flag_denied_list: parse_env_list(deny_list)?,
            prompt,
            ..Default::default()
        })
    }

    pub fn new_sys(
        allow_list: &Option<Vec<String>>,
        deny_list: &Option<Vec<String>>,
        prompt: bool,
    ) -> Result<UnaryPermission<SysDescriptor>, AnyError> {
        Ok(UnaryPermission::<SysDescriptor> {
            granted_global: global_from_option(allow_list),
            granted_list: parse_sys_list(allow_list)?,
            flag_denied_global: global_from_option(deny_list),
            flag_denied_list: parse_sys_list(deny_list)?,
            prompt,
            ..Default::default()
        })
    }

    pub fn new_run(
        allow_list: &Option<Vec<String>>,
        deny_list: &Option<Vec<String>>,
        prompt: bool,
    ) -> Result<UnaryPermission<RunDescriptor>, AnyError> {
        Ok(UnaryPermission::<RunDescriptor> {
            granted_global: global_from_option(allow_list),
            granted_list: parse_run_list(allow_list)?,
            flag_denied_global: global_from_option(deny_list),
            flag_denied_list: parse_run_list(deny_list)?,
            prompt,
            ..Default::default()
        })
    }

    pub fn new_ffi(
        allow_list: &Option<Vec<PathBuf>>,
        deny_list: &Option<Vec<PathBuf>>,
        prompt: bool,
    ) -> Result<UnaryPermission<FfiDescriptor>, AnyError> {
        Ok(UnaryPermission::<FfiDescriptor> {
            granted_global: global_from_option(allow_list),
            granted_list: parse_path_list(allow_list, FfiDescriptor)?,
            flag_denied_global: global_from_option(deny_list),
            flag_denied_list: parse_path_list(deny_list, FfiDescriptor)?,
            prompt,
            ..Default::default()
        })
    }

    pub fn new_hrtime(allow_state: bool, deny_state: bool) -> UnitPermission {
        unit_permission_from_flag_bools(
            allow_state,
            deny_state,
            "hrtime",
            "high precision time",
            false, // never prompt for hrtime
        )
    }

    #[allow(dead_code)]
    pub fn from_options(opts: &PermissionsOptions) -> Result<Self, AnyError> {
        Ok(Self {
            read: Permissions::new_read(&opts.allow_read, &opts.deny_read, opts.prompt)?,
            write: Permissions::new_write(&opts.allow_write, &opts.deny_write, opts.prompt)?,
            net: Permissions::new_net(&opts.allow_net, &opts.deny_net, opts.prompt)?,
            env: Permissions::new_env(&opts.allow_env, &opts.deny_env, opts.prompt)?,
            sys: Permissions::new_sys(&opts.allow_sys, &opts.deny_sys, opts.prompt)?,
            run: Permissions::new_run(&opts.allow_run, &opts.deny_run, opts.prompt)?,
            ffi: Permissions::new_ffi(&opts.allow_ffi, &opts.deny_ffi, opts.prompt)?,
            hrtime: Permissions::new_hrtime(opts.allow_hrtime, opts.deny_hrtime),
        })
    }

    #[allow(dead_code)]
    pub fn allow_all() -> Self {
        Self {
            read: Permissions::new_read(&Some(vec![]), &None, false).unwrap(),
            write: Permissions::new_write(&Some(vec![]), &None, false).unwrap(),
            net: Permissions::new_net(&Some(vec![]), &None, false).unwrap(),
            env: Permissions::new_env(&Some(vec![]), &None, false).unwrap(),
            sys: Permissions::new_sys(&Some(vec![]), &None, false).unwrap(),
            run: Permissions::new_run(&Some(vec![]), &None, false).unwrap(),
            ffi: Permissions::new_ffi(&Some(vec![]), &None, false).unwrap(),
            hrtime: Permissions::new_hrtime(true, false),
        }
    }

    pub fn allow_net(allow_list: Vec<String>, deny_list: Option<Vec<String>>) -> Self {
        Self {
            read: Permissions::new_read(&None, &None, false).unwrap(),
            write: Permissions::new_write(&None, &None, false).unwrap(),
            net: Permissions::new_net(&Some(allow_list), &deny_list, false).unwrap(),
            env: Permissions::new_env(&None, &None, false).unwrap(),
            sys: Permissions::new_sys(&None, &None, false).unwrap(),
            run: Permissions::new_run(&None, &None, false).unwrap(),
            ffi: Permissions::new_ffi(&None, &None, false).unwrap(),
            hrtime: Permissions::new_hrtime(false, false),
        }
    }

    /// A helper function that determines if the module specifier is a local or
    /// remote, and performs a read or net check for the specifier.
    #[allow(dead_code)]
    pub fn check_specifier(&mut self, specifier: &ModuleSpecifier) -> Result<(), AnyError> {
        match specifier.scheme() {
            "file" => match specifier.to_file_path() {
                Ok(path) => self.read.check(&path, Some("import()")),
                Err(_) => Err(uri_error(format!(
                    "Invalid file path.\n  Specifier: {specifier}"
                ))),
            },
            "data" => Ok(()),
            "blob" => Ok(()),
            _ => self.net.check_url(specifier, Some("import()")),
        }
    }
}

/// Wrapper struct for `Permissions` that can be shared across threads.
///
/// We need a way to have internal mutability for permissions as they might get
/// passed to a future that will prompt the user for permission (and in such
/// case might need to be mutated). Also for the Web Worker API we need a way
/// to send permissions to a new thread.
#[derive(Clone, Debug)]
pub struct PermissionsContainer(pub Arc<Mutex<Permissions>>);

#[allow(dead_code)]
impl PermissionsContainer {
    pub fn new(perms: Permissions) -> Self {
        Self(Arc::new(Mutex::new(perms)))
    }

    pub fn allow_all() -> Self {
        Self::new(Permissions::allow_all())
    }

    pub fn allow_net(allow_list: Vec<String>, deny_list: Option<Vec<String>>) -> Self {
        Self::new(Permissions::allow_net(allow_list, deny_list))
    }

    #[inline(always)]
    pub fn check_specifier(&self, specifier: &ModuleSpecifier) -> Result<(), AnyError> {
        self.0.lock().check_specifier(specifier)
    }

    #[inline(always)]
    pub fn check_read(&mut self, path: &Path, api_name: &str) -> Result<(), AnyError> {
        self.0.lock().read.check(path, Some(api_name))
    }

    #[inline(always)]
    pub fn check_read_blind(
        &mut self,
        path: &Path,
        display: &str,
        api_name: &str,
    ) -> Result<(), AnyError> {
        self.0.lock().read.check_blind(path, display, api_name)
    }

    #[inline(always)]
    pub fn check_read_all(&mut self, api_name: &str) -> Result<(), AnyError> {
        self.0.lock().read.check_all(Some(api_name))
    }

    #[inline(always)]
    pub fn check_write(&mut self, path: &Path, api_name: &str) -> Result<(), AnyError> {
        self.0.lock().write.check(path, Some(api_name))
    }

    #[inline(always)]
    pub fn check_write_all(&mut self, api_name: &str) -> Result<(), AnyError> {
        self.0.lock().write.check_all(Some(api_name))
    }

    #[inline(always)]
    pub fn check_write_blind(
        &mut self,
        path: &Path,
        display: &str,
        api_name: &str,
    ) -> Result<(), AnyError> {
        self.0.lock().write.check_blind(path, display, api_name)
    }

    #[inline(always)]
    pub fn check_run(&mut self, cmd: &str, api_name: &str) -> Result<(), AnyError> {
        self.0.lock().run.check(cmd, Some(api_name))
    }

    #[inline(always)]
    pub fn check_run_all(&mut self, api_name: &str) -> Result<(), AnyError> {
        self.0.lock().run.check_all(Some(api_name))
    }

    #[inline(always)]
    pub fn check_sys(&mut self, kind: &str, api_name: &str) -> Result<(), AnyError> {
        self.0.lock().sys.check(kind, Some(api_name))
    }

    #[inline(always)]
    pub fn check_env(&mut self, var: &str) -> Result<(), AnyError> {
        self.0.lock().env.check(var)
    }

    #[inline(always)]
    pub fn check_env_all(&mut self) -> Result<(), AnyError> {
        self.0.lock().env.check_all()
    }
}

impl deno_net::NetPermissions for PermissionsContainer {
    #[inline(always)]
    fn check_net<T: AsRef<str>>(
        &mut self,
        host: &(T, Option<u16>),
        api_name: &str,
    ) -> Result<(), AnyError> {
        self.0.lock().net.check(host, Some(api_name))
    }

    #[inline(always)]
    fn check_read(&mut self, path: &Path, api_name: &str) -> Result<(), AnyError> {
        self.0.lock().read.check(path, Some(api_name))
    }

    #[inline(always)]
    fn check_write(&mut self, path: &Path, api_name: &str) -> Result<(), AnyError> {
        self.0.lock().write.check(path, Some(api_name))
    }
}

#[cfg(feature = "with-fetch")]
impl deno_fetch::FetchPermissions for PermissionsContainer {
    #[inline(always)]
    fn check_net_url(&mut self, url: &url::Url, api_name: &str) -> Result<(), AnyError> {
        self.0.lock().net.check_url(url, Some(api_name))
    }

    #[inline(always)]
    fn check_read(&mut self, path: &Path, api_name: &str) -> Result<(), AnyError> {
        self.0.lock().read.check(path, Some(api_name))
    }
}

impl deno_web::TimersPermission for PermissionsContainer {
    #[inline(always)]
    fn allow_hrtime(&mut self) -> bool {
        self.0.lock().hrtime.check().is_ok()
    }
}

#[cfg(feature = "with-fetch")]
impl deno_websocket::WebSocketPermissions for PermissionsContainer {
    #[inline(always)]
    fn check_net_url(&mut self, url: &url::Url, api_name: &str) -> Result<(), AnyError> {
        self.0.lock().net.check_url(url, Some(api_name))
    }
}

fn unit_permission_from_flag_bools(
    allow_flag: bool,
    deny_flag: bool,
    name: &'static str,
    description: &'static str,
    prompt: bool,
) -> UnitPermission {
    UnitPermission {
        name,
        description,
        state: if deny_flag {
            PermissionState::Denied
        } else if allow_flag {
            PermissionState::Granted
        } else {
            PermissionState::Prompt
        },
        prompt,
    }
}

fn global_from_option<T>(flag: &Option<Vec<T>>) -> bool {
    matches!(flag, Some(v) if v.is_empty())
}

fn parse_net_list(list: &Option<Vec<String>>) -> Result<HashSet<NetDescriptor>, AnyError> {
    if let Some(v) = list {
        v.iter()
            .map(|x| NetDescriptor::from_str(x))
            .collect::<Result<HashSet<NetDescriptor>, AnyError>>()
    } else {
        Ok(HashSet::new())
    }
}

fn parse_env_list(list: &Option<Vec<String>>) -> Result<HashSet<EnvDescriptor>, AnyError> {
    if let Some(v) = list {
        v.iter()
            .map(|x| {
                if x.is_empty() {
                    Err(AnyError::msg("Empty path is not allowed"))
                } else {
                    Ok(EnvDescriptor::new(x))
                }
            })
            .collect()
    } else {
        Ok(HashSet::new())
    }
}

fn parse_path_list<T: Descriptor + Hash>(
    list: &Option<Vec<PathBuf>>,
    f: fn(PathBuf) -> T,
) -> Result<HashSet<T>, AnyError> {
    if let Some(v) = list {
        v.iter()
            .map(|raw_path| {
                if raw_path.as_os_str().is_empty() {
                    Err(AnyError::msg("Empty path is not allowed"))
                } else {
                    resolve_from_cwd(Path::new(&raw_path)).map(f)
                }
            })
            .collect()
    } else {
        Ok(HashSet::new())
    }
}

fn parse_sys_list(list: &Option<Vec<String>>) -> Result<HashSet<SysDescriptor>, AnyError> {
    if let Some(v) = list {
        v.iter()
            .map(|x| {
                if x.is_empty() {
                    Err(AnyError::msg("empty"))
                } else {
                    Ok(SysDescriptor(x.to_string()))
                }
            })
            .collect()
    } else {
        Ok(HashSet::new())
    }
}

fn parse_run_list(list: &Option<Vec<String>>) -> Result<HashSet<RunDescriptor>, AnyError> {
    if let Some(v) = list {
        v.iter()
            .map(|x| {
                if x.is_empty() {
                    Err(AnyError::msg("Empty path is not allowed"))
                } else {
                    Ok(RunDescriptor::from_str(x).unwrap())
                }
            })
            .collect()
    } else {
        Ok(HashSet::new())
    }
}

fn escalation_error() -> AnyError {
    custom_error(
        "PermissionDenied",
        "Can't escalate parent thread permissions",
    )
}

#[derive(Debug, Eq, PartialEq)]
pub enum ChildUnitPermissionArg {
    Inherit,
    Granted,
    NotGranted,
}

impl<'de> Deserialize<'de> for ChildUnitPermissionArg {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ChildUnitPermissionArgVisitor;
        impl<'de> de::Visitor<'de> for ChildUnitPermissionArgVisitor {
            type Value = ChildUnitPermissionArg;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("\"inherit\" or boolean")
            }

            fn visit_unit<E>(self) -> Result<ChildUnitPermissionArg, E>
            where
                E: de::Error,
            {
                Ok(ChildUnitPermissionArg::NotGranted)
            }

            fn visit_str<E>(self, v: &str) -> Result<ChildUnitPermissionArg, E>
            where
                E: de::Error,
            {
                if v == "inherit" {
                    Ok(ChildUnitPermissionArg::Inherit)
                } else {
                    Err(de::Error::invalid_value(de::Unexpected::Str(v), &self))
                }
            }

            fn visit_bool<E>(self, v: bool) -> Result<ChildUnitPermissionArg, E>
            where
                E: de::Error,
            {
                match v {
                    true => Ok(ChildUnitPermissionArg::Granted),
                    false => Ok(ChildUnitPermissionArg::NotGranted),
                }
            }
        }
        deserializer.deserialize_any(ChildUnitPermissionArgVisitor)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum ChildUnaryPermissionArg {
    Inherit,
    Granted,
    NotGranted,
    GrantedList(Vec<String>),
}

impl<'de> Deserialize<'de> for ChildUnaryPermissionArg {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ChildUnaryPermissionArgVisitor;
        impl<'de> de::Visitor<'de> for ChildUnaryPermissionArgVisitor {
            type Value = ChildUnaryPermissionArg;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("\"inherit\" or boolean or string[]")
            }

            fn visit_unit<E>(self) -> Result<ChildUnaryPermissionArg, E>
            where
                E: de::Error,
            {
                Ok(ChildUnaryPermissionArg::NotGranted)
            }

            fn visit_str<E>(self, v: &str) -> Result<ChildUnaryPermissionArg, E>
            where
                E: de::Error,
            {
                if v == "inherit" {
                    Ok(ChildUnaryPermissionArg::Inherit)
                } else {
                    Err(de::Error::invalid_value(de::Unexpected::Str(v), &self))
                }
            }

            fn visit_bool<E>(self, v: bool) -> Result<ChildUnaryPermissionArg, E>
            where
                E: de::Error,
            {
                match v {
                    true => Ok(ChildUnaryPermissionArg::Granted),
                    false => Ok(ChildUnaryPermissionArg::NotGranted),
                }
            }

            fn visit_seq<V>(self, mut v: V) -> Result<ChildUnaryPermissionArg, V::Error>
            where
                V: de::SeqAccess<'de>,
            {
                let mut granted_list = vec![];
                while let Some(value) = v.next_element::<String>()? {
                    granted_list.push(value);
                }
                Ok(ChildUnaryPermissionArg::GrantedList(granted_list))
            }
        }
        deserializer.deserialize_any(ChildUnaryPermissionArgVisitor)
    }
}

/// Directly deserializable from JS worker and test permission options.
#[derive(Debug, Eq, PartialEq)]
pub struct ChildPermissionsArg {
    env: ChildUnaryPermissionArg,
    hrtime: ChildUnitPermissionArg,
    net: ChildUnaryPermissionArg,
    ffi: ChildUnaryPermissionArg,
    read: ChildUnaryPermissionArg,
    run: ChildUnaryPermissionArg,
    sys: ChildUnaryPermissionArg,
    write: ChildUnaryPermissionArg,
}

impl ChildPermissionsArg {
    pub fn inherit() -> Self {
        ChildPermissionsArg {
            env: ChildUnaryPermissionArg::Inherit,
            hrtime: ChildUnitPermissionArg::Inherit,
            net: ChildUnaryPermissionArg::Inherit,
            ffi: ChildUnaryPermissionArg::Inherit,
            read: ChildUnaryPermissionArg::Inherit,
            run: ChildUnaryPermissionArg::Inherit,
            sys: ChildUnaryPermissionArg::Inherit,
            write: ChildUnaryPermissionArg::Inherit,
        }
    }

    pub fn none() -> Self {
        ChildPermissionsArg {
            env: ChildUnaryPermissionArg::NotGranted,
            hrtime: ChildUnitPermissionArg::NotGranted,
            net: ChildUnaryPermissionArg::NotGranted,
            ffi: ChildUnaryPermissionArg::NotGranted,
            read: ChildUnaryPermissionArg::NotGranted,
            run: ChildUnaryPermissionArg::NotGranted,
            sys: ChildUnaryPermissionArg::NotGranted,
            write: ChildUnaryPermissionArg::NotGranted,
        }
    }
}

impl<'de> Deserialize<'de> for ChildPermissionsArg {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ChildPermissionsArgVisitor;
        impl<'de> de::Visitor<'de> for ChildPermissionsArgVisitor {
            type Value = ChildPermissionsArg;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("\"inherit\" or \"none\" or object")
            }

            fn visit_unit<E>(self) -> Result<ChildPermissionsArg, E>
            where
                E: de::Error,
            {
                Ok(ChildPermissionsArg::inherit())
            }

            fn visit_str<E>(self, v: &str) -> Result<ChildPermissionsArg, E>
            where
                E: de::Error,
            {
                if v == "inherit" {
                    Ok(ChildPermissionsArg::inherit())
                } else if v == "none" {
                    Ok(ChildPermissionsArg::none())
                } else {
                    Err(de::Error::invalid_value(de::Unexpected::Str(v), &self))
                }
            }

            fn visit_map<V>(self, mut v: V) -> Result<ChildPermissionsArg, V::Error>
            where
                V: de::MapAccess<'de>,
            {
                let mut child_permissions_arg = ChildPermissionsArg::none();
                while let Some((key, value)) = v.next_entry::<String, serde_json::Value>()? {
                    if key == "env" {
                        let arg = serde_json::from_value::<ChildUnaryPermissionArg>(value);
                        child_permissions_arg.env = arg.map_err(|e| {
                            de::Error::custom(format!("(deno.permissions.env) {e}"))
                        })?;
                    } else if key == "hrtime" {
                        let arg = serde_json::from_value::<ChildUnitPermissionArg>(value);
                        child_permissions_arg.hrtime = arg.map_err(|e| {
                            de::Error::custom(format!("(deno.permissions.hrtime) {e}"))
                        })?;
                    } else if key == "net" {
                        let arg = serde_json::from_value::<ChildUnaryPermissionArg>(value);
                        child_permissions_arg.net = arg.map_err(|e| {
                            de::Error::custom(format!("(deno.permissions.net) {e}"))
                        })?;
                    } else if key == "ffi" {
                        let arg = serde_json::from_value::<ChildUnaryPermissionArg>(value);
                        child_permissions_arg.ffi = arg.map_err(|e| {
                            de::Error::custom(format!("(deno.permissions.ffi) {e}"))
                        })?;
                    } else if key == "read" {
                        let arg = serde_json::from_value::<ChildUnaryPermissionArg>(value);
                        child_permissions_arg.read = arg.map_err(|e| {
                            de::Error::custom(format!("(deno.permissions.read) {e}"))
                        })?;
                    } else if key == "run" {
                        let arg = serde_json::from_value::<ChildUnaryPermissionArg>(value);
                        child_permissions_arg.run = arg.map_err(|e| {
                            de::Error::custom(format!("(deno.permissions.run) {e}"))
                        })?;
                    } else if key == "sys" {
                        let arg = serde_json::from_value::<ChildUnaryPermissionArg>(value);
                        child_permissions_arg.sys = arg.map_err(|e| {
                            de::Error::custom(format!("(deno.permissions.sys) {e}"))
                        })?;
                    } else if key == "write" {
                        let arg = serde_json::from_value::<ChildUnaryPermissionArg>(value);
                        child_permissions_arg.write = arg.map_err(|e| {
                            de::Error::custom(format!("(deno.permissions.write) {e}"))
                        })?;
                    } else {
                        return Err(de::Error::custom("unknown permission name"));
                    }
                }
                Ok(child_permissions_arg)
            }
        }
        deserializer.deserialize_any(ChildPermissionsArgVisitor)
    }
}

#[allow(dead_code)]
pub fn create_child_permissions(
    main_perms: &mut Permissions,
    child_permissions_arg: ChildPermissionsArg,
) -> Result<Permissions, AnyError> {
    let mut worker_perms = Permissions::default();
    match child_permissions_arg.env {
        ChildUnaryPermissionArg::Inherit => {
            worker_perms.env = main_perms.env.clone();
        }
        ChildUnaryPermissionArg::Granted => {
            if main_perms.env.check_all().is_err() {
                return Err(escalation_error());
            }
            worker_perms.env.granted_global = true;
        }
        ChildUnaryPermissionArg::NotGranted => {}
        ChildUnaryPermissionArg::GrantedList(granted_list) => {
            worker_perms.env.granted_list = parse_env_list(&Some(granted_list))?;
            if !worker_perms
                .env
                .granted_list
                .iter()
                .all(|desc| main_perms.env.check(desc.as_ref()).is_ok())
            {
                return Err(escalation_error());
            }
        }
    }
    worker_perms.env.flag_denied_global = main_perms.env.flag_denied_global;
    worker_perms.env.flag_denied_list = main_perms.env.flag_denied_list.clone();
    worker_perms.env.prompt_denied_global = main_perms.env.prompt_denied_global;
    worker_perms.env.prompt_denied_list = main_perms.env.prompt_denied_list.clone();
    worker_perms.env.prompt = main_perms.env.prompt;
    match child_permissions_arg.sys {
        ChildUnaryPermissionArg::Inherit => {
            worker_perms.sys = main_perms.sys.clone();
        }
        ChildUnaryPermissionArg::Granted => {
            if main_perms.sys.check_all().is_err() {
                return Err(escalation_error());
            }
            worker_perms.sys.granted_global = true;
        }
        ChildUnaryPermissionArg::NotGranted => {}
        ChildUnaryPermissionArg::GrantedList(granted_list) => {
            worker_perms.sys.granted_list = parse_sys_list(&Some(granted_list))?;
            if !worker_perms
                .sys
                .granted_list
                .iter()
                .all(|desc| main_perms.sys.check(&desc.0, None).is_ok())
            {
                return Err(escalation_error());
            }
        }
    }
    worker_perms.sys.flag_denied_global = main_perms.sys.flag_denied_global;
    worker_perms.sys.flag_denied_list = main_perms.sys.flag_denied_list.clone();
    worker_perms.sys.prompt_denied_global = main_perms.sys.prompt_denied_global;
    worker_perms.sys.prompt_denied_list = main_perms.sys.prompt_denied_list.clone();
    worker_perms.sys.prompt = main_perms.sys.prompt;
    match child_permissions_arg.hrtime {
        ChildUnitPermissionArg::Inherit => {
            worker_perms.hrtime = main_perms.hrtime.clone();
        }
        ChildUnitPermissionArg::Granted => {
            if main_perms.hrtime.check().is_err() {
                return Err(escalation_error());
            }
            worker_perms.hrtime.state = PermissionState::Granted;
        }
        ChildUnitPermissionArg::NotGranted => {}
    }
    if main_perms.hrtime.state == PermissionState::Denied {
        worker_perms.hrtime.state = PermissionState::Denied;
    }
    worker_perms.hrtime.prompt = main_perms.hrtime.prompt;
    match child_permissions_arg.net {
        ChildUnaryPermissionArg::Inherit => {
            worker_perms.net = main_perms.net.clone();
        }
        ChildUnaryPermissionArg::Granted => {
            if main_perms.net.check_all().is_err() {
                return Err(escalation_error());
            }
            worker_perms.net.granted_global = true;
        }
        ChildUnaryPermissionArg::NotGranted => {}
        ChildUnaryPermissionArg::GrantedList(granted_list) => {
            worker_perms.net.granted_list = parse_net_list(&Some(granted_list))?;
            if !worker_perms
                .net
                .granted_list
                .iter()
                .all(|desc| main_perms.net.check(&(&desc.0, desc.1), None).is_ok())
            {
                return Err(escalation_error());
            }
        }
    }
    worker_perms.net.flag_denied_global = main_perms.net.flag_denied_global;
    worker_perms.net.flag_denied_list = main_perms.net.flag_denied_list.clone();
    worker_perms.net.prompt_denied_global = main_perms.net.prompt_denied_global;
    worker_perms.net.prompt_denied_list = main_perms.net.prompt_denied_list.clone();
    worker_perms.net.prompt = main_perms.net.prompt;
    match child_permissions_arg.ffi {
        ChildUnaryPermissionArg::Inherit => {
            worker_perms.ffi = main_perms.ffi.clone();
        }
        ChildUnaryPermissionArg::Granted => {
            if main_perms.ffi.check_all().is_err() {
                return Err(escalation_error());
            }
            worker_perms.ffi.granted_global = true;
        }
        ChildUnaryPermissionArg::NotGranted => {}
        ChildUnaryPermissionArg::GrantedList(granted_list) => {
            worker_perms.ffi.granted_list = parse_path_list(
                &Some(granted_list.iter().map(PathBuf::from).collect()),
                FfiDescriptor,
            )?;
            if !worker_perms
                .ffi
                .granted_list
                .iter()
                .all(|desc| main_perms.ffi.check(&desc.0, None).is_ok())
            {
                return Err(escalation_error());
            }
        }
    }
    worker_perms.ffi.flag_denied_global = main_perms.env.flag_denied_global;
    worker_perms.ffi.flag_denied_list = main_perms.ffi.flag_denied_list.clone();
    worker_perms.ffi.prompt_denied_global = main_perms.ffi.prompt_denied_global;
    worker_perms.ffi.prompt_denied_list = main_perms.ffi.prompt_denied_list.clone();
    worker_perms.ffi.prompt = main_perms.ffi.prompt;
    match child_permissions_arg.read {
        ChildUnaryPermissionArg::Inherit => {
            worker_perms.read = main_perms.read.clone();
        }
        ChildUnaryPermissionArg::Granted => {
            if main_perms.read.check_all(None).is_err() {
                return Err(escalation_error());
            }
            worker_perms.read.granted_global = true;
        }
        ChildUnaryPermissionArg::NotGranted => {}
        ChildUnaryPermissionArg::GrantedList(granted_list) => {
            worker_perms.read.granted_list = parse_path_list(
                &Some(granted_list.iter().map(PathBuf::from).collect()),
                ReadDescriptor,
            )?;
            if !worker_perms
                .read
                .granted_list
                .iter()
                .all(|desc| main_perms.read.check(&desc.0, None).is_ok())
            {
                return Err(escalation_error());
            }
        }
    }
    worker_perms.read.flag_denied_global = main_perms.read.flag_denied_global;
    worker_perms.read.flag_denied_list = main_perms.read.flag_denied_list.clone();
    worker_perms.read.prompt_denied_global = main_perms.read.prompt_denied_global;
    worker_perms.read.prompt_denied_list = main_perms.read.prompt_denied_list.clone();
    worker_perms.read.prompt = main_perms.read.prompt;
    match child_permissions_arg.run {
        ChildUnaryPermissionArg::Inherit => {
            worker_perms.run = main_perms.run.clone();
        }
        ChildUnaryPermissionArg::Granted => {
            if main_perms.run.check_all(None).is_err() {
                return Err(escalation_error());
            }
            worker_perms.run.granted_global = true;
        }
        ChildUnaryPermissionArg::NotGranted => {}
        ChildUnaryPermissionArg::GrantedList(granted_list) => {
            worker_perms.run.granted_list = parse_run_list(&Some(granted_list))?;
            if !worker_perms
                .run
                .granted_list
                .iter()
                .all(|desc| main_perms.run.check(&desc.to_string(), None).is_ok())
            {
                return Err(escalation_error());
            }
        }
    }
    worker_perms.run.flag_denied_global = main_perms.run.flag_denied_global;
    worker_perms.run.flag_denied_list = main_perms.run.flag_denied_list.clone();
    worker_perms.run.prompt_denied_global = main_perms.run.prompt_denied_global;
    worker_perms.run.prompt_denied_list = main_perms.run.prompt_denied_list.clone();
    worker_perms.run.prompt = main_perms.run.prompt;
    match child_permissions_arg.write {
        ChildUnaryPermissionArg::Inherit => {
            worker_perms.write = main_perms.write.clone();
        }
        ChildUnaryPermissionArg::Granted => {
            if main_perms.write.check_all(None).is_err() {
                return Err(escalation_error());
            }
            worker_perms.write.granted_global = true;
        }
        ChildUnaryPermissionArg::NotGranted => {}
        ChildUnaryPermissionArg::GrantedList(granted_list) => {
            worker_perms.write.granted_list = parse_path_list(
                &Some(granted_list.iter().map(PathBuf::from).collect()),
                WriteDescriptor,
            )?;
            if !worker_perms
                .write
                .granted_list
                .iter()
                .all(|desc| main_perms.write.check(&desc.0, None).is_ok())
            {
                return Err(escalation_error());
            }
        }
    }
    worker_perms.write.flag_denied_global = main_perms.write.flag_denied_global;
    worker_perms.write.flag_denied_list = main_perms.write.flag_denied_list.clone();
    worker_perms.write.prompt_denied_global = main_perms.write.prompt_denied_global;
    worker_perms.write.prompt_denied_list = main_perms.write.prompt_denied_list.clone();
    worker_perms.write.prompt = main_perms.write.prompt;
    Ok(worker_perms)
}

pub fn resolve_from_cwd(path: &Path) -> Result<PathBuf, AnyError> {
    let resolved_path = if path.is_absolute() {
        path.to_owned()
    } else {
        let cwd = current_dir().context("Failed to get current working directory")?;
        cwd.join(path)
    };

    Ok(normalize_path(resolved_path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use deno_core::serde_json::json;

    // Creates vector of strings, Vec<String>
    macro_rules! svec {
      ($($x:expr),*) => (vec![$($x.to_string()),*]);
  }

    #[test]
    fn check_paths() {
        let allowlist = vec![
            PathBuf::from("/a/specific/dir/name"),
            PathBuf::from("/a/specific"),
            PathBuf::from("/b/c"),
        ];

        let mut perms = Permissions::from_options(&PermissionsOptions {
            allow_read: Some(allowlist.clone()),
            allow_write: Some(allowlist.clone()),
            allow_ffi: Some(allowlist),
            ..Default::default()
        })
        .unwrap();

        // Inside of /a/specific and /a/specific/dir/name
        assert!(perms
            .read
            .check(Path::new("/a/specific/dir/name"), None)
            .is_ok());
        assert!(perms
            .write
            .check(Path::new("/a/specific/dir/name"), None)
            .is_ok());
        assert!(perms
            .ffi
            .check(Path::new("/a/specific/dir/name"), None)
            .is_ok());

        // Inside of /a/specific but outside of /a/specific/dir/name
        assert!(perms.read.check(Path::new("/a/specific/dir"), None).is_ok());
        assert!(perms
            .write
            .check(Path::new("/a/specific/dir"), None)
            .is_ok());
        assert!(perms.ffi.check(Path::new("/a/specific/dir"), None).is_ok());

        // Inside of /a/specific and /a/specific/dir/name
        assert!(perms
            .read
            .check(Path::new("/a/specific/dir/name/inner"), None)
            .is_ok());
        assert!(perms
            .write
            .check(Path::new("/a/specific/dir/name/inner"), None)
            .is_ok());
        assert!(perms
            .ffi
            .check(Path::new("/a/specific/dir/name/inner"), None)
            .is_ok());

        // Inside of /a/specific but outside of /a/specific/dir/name
        assert!(perms
            .read
            .check(Path::new("/a/specific/other/dir"), None)
            .is_ok());
        assert!(perms
            .write
            .check(Path::new("/a/specific/other/dir"), None)
            .is_ok());
        assert!(perms
            .ffi
            .check(Path::new("/a/specific/other/dir"), None)
            .is_ok());

        // Exact match with /b/c
        assert!(perms.read.check(Path::new("/b/c"), None).is_ok());
        assert!(perms.write.check(Path::new("/b/c"), None).is_ok());
        assert!(perms.ffi.check(Path::new("/b/c"), None).is_ok());

        // Sub path within /b/c
        assert!(perms.read.check(Path::new("/b/c/sub/path"), None).is_ok());
        assert!(perms.write.check(Path::new("/b/c/sub/path"), None).is_ok());
        assert!(perms.ffi.check(Path::new("/b/c/sub/path"), None).is_ok());

        // Sub path within /b/c, needs normalizing
        assert!(perms
            .read
            .check(Path::new("/b/c/sub/path/../path/."), None)
            .is_ok());
        assert!(perms
            .write
            .check(Path::new("/b/c/sub/path/../path/."), None)
            .is_ok());
        assert!(perms
            .ffi
            .check(Path::new("/b/c/sub/path/../path/."), None)
            .is_ok());

        // Inside of /b but outside of /b/c
        assert!(perms.read.check(Path::new("/b/e"), None).is_err());
        assert!(perms.write.check(Path::new("/b/e"), None).is_err());
        assert!(perms.ffi.check(Path::new("/b/e"), None).is_err());

        // Inside of /a but outside of /a/specific
        assert!(perms.read.check(Path::new("/a/b"), None).is_err());
        assert!(perms.write.check(Path::new("/a/b"), None).is_err());
        assert!(perms.ffi.check(Path::new("/a/b"), None).is_err());
    }

    #[test]
    fn test_check_net_with_values() {
        let mut perms = Permissions::from_options(&PermissionsOptions {
            allow_net: Some(svec![
                "localhost",
                "deno.land",
                "github.com:3000",
                "127.0.0.1",
                "172.16.0.2:8000",
                "www.github.com:443"
            ]),
            ..Default::default()
        })
        .unwrap();

        let domain_tests = vec![
            ("localhost", 1234, true),
            ("deno.land", 0, true),
            ("deno.land", 3000, true),
            ("deno.lands", 0, false),
            ("deno.lands", 3000, false),
            ("github.com", 3000, true),
            ("github.com", 0, false),
            ("github.com", 2000, false),
            ("github.net", 3000, false),
            ("127.0.0.1", 0, true),
            ("127.0.0.1", 3000, true),
            ("127.0.0.2", 0, false),
            ("127.0.0.2", 3000, false),
            ("172.16.0.2", 8000, true),
            ("172.16.0.2", 0, false),
            ("172.16.0.2", 6000, false),
            ("172.16.0.1", 8000, false),
            // Just some random hosts that should err
            ("somedomain", 0, false),
            ("192.168.0.1", 0, false),
        ];

        for (host, port, is_ok) in domain_tests {
            assert_eq!(
                is_ok,
                perms.net.check(&(host, Some(port)), None).is_ok(),
                "{}:{}",
                host,
                port,
            );
        }
    }

    #[test]
    fn test_check_net_only_flag() {
        let mut perms = Permissions::from_options(&PermissionsOptions {
            allow_net: Some(svec![]), // this means `--allow-net` is present without values following `=` sign
            ..Default::default()
        })
        .unwrap();

        let domain_tests = vec![
            ("localhost", 1234),
            ("deno.land", 0),
            ("deno.land", 3000),
            ("deno.lands", 0),
            ("deno.lands", 3000),
            ("github.com", 3000),
            ("github.com", 0),
            ("github.com", 2000),
            ("github.net", 3000),
            ("127.0.0.1", 0),
            ("127.0.0.1", 3000),
            ("127.0.0.2", 0),
            ("127.0.0.2", 3000),
            ("172.16.0.2", 8000),
            ("172.16.0.2", 0),
            ("172.16.0.2", 6000),
            ("172.16.0.1", 8000),
            ("somedomain", 0),
            ("192.168.0.1", 0),
        ];

        for (host, port) in domain_tests {
            assert!(perms.net.check(&(host, Some(port)), None).is_ok());
        }
    }

    #[test]
    fn test_check_net_no_flag() {
        let mut perms = Permissions::from_options(&PermissionsOptions {
            allow_net: None,
            ..Default::default()
        })
        .unwrap();

        let domain_tests = vec![
            ("localhost", 1234),
            ("deno.land", 0),
            ("deno.land", 3000),
            ("deno.lands", 0),
            ("deno.lands", 3000),
            ("github.com", 3000),
            ("github.com", 0),
            ("github.com", 2000),
            ("github.net", 3000),
            ("127.0.0.1", 0),
            ("127.0.0.1", 3000),
            ("127.0.0.2", 0),
            ("127.0.0.2", 3000),
            ("172.16.0.2", 8000),
            ("172.16.0.2", 0),
            ("172.16.0.2", 6000),
            ("172.16.0.1", 8000),
            ("somedomain", 0),
            ("192.168.0.1", 0),
        ];

        for (host, port) in domain_tests {
            assert!(perms.net.check(&(host, Some(port)), None).is_err());
        }
    }

    #[test]
    fn test_check_net_url() {
        let mut perms = Permissions::from_options(&PermissionsOptions {
            allow_net: Some(svec![
                "localhost",
                "deno.land",
                "github.com:3000",
                "127.0.0.1",
                "172.16.0.2:8000",
                "www.github.com:443"
            ]),
            ..Default::default()
        })
        .unwrap();

        let url_tests = vec![
            // Any protocol + port for localhost should be ok, since we don't specify
            ("http://localhost", true),
            ("https://localhost", true),
            ("https://localhost:4443", true),
            ("tcp://localhost:5000", true),
            ("udp://localhost:6000", true),
            // Correct domain + any port and protocol should be ok incorrect shouldn't
            ("https://deno.land/std/example/welcome.ts", true),
            ("https://deno.land:3000/std/example/welcome.ts", true),
            ("https://deno.lands/std/example/welcome.ts", false),
            ("https://deno.lands:3000/std/example/welcome.ts", false),
            // Correct domain + port should be ok all other combinations should err
            ("https://github.com:3000/denoland/deno", true),
            ("https://github.com/denoland/deno", false),
            ("https://github.com:2000/denoland/deno", false),
            ("https://github.net:3000/denoland/deno", false),
            // Correct ipv4 address + any port should be ok others should err
            ("tcp://127.0.0.1", true),
            ("https://127.0.0.1", true),
            ("tcp://127.0.0.1:3000", true),
            ("https://127.0.0.1:3000", true),
            ("tcp://127.0.0.2", false),
            ("https://127.0.0.2", false),
            ("tcp://127.0.0.2:3000", false),
            ("https://127.0.0.2:3000", false),
            // Correct address + port should be ok all other combinations should err
            ("tcp://172.16.0.2:8000", true),
            ("https://172.16.0.2:8000", true),
            ("tcp://172.16.0.2", false),
            ("https://172.16.0.2", false),
            ("tcp://172.16.0.2:6000", false),
            ("https://172.16.0.2:6000", false),
            ("tcp://172.16.0.1:8000", false),
            ("https://172.16.0.1:8000", false),
            // Testing issue #6531 (Network permissions check doesn't account for well-known default ports) so we dont regress
            ("https://www.github.com:443/robots.txt", true),
        ];

        for (url_str, is_ok) in url_tests {
            let u = url::Url::parse(url_str).unwrap();
            assert_eq!(is_ok, perms.net.check_url(&u, None).is_ok(), "{}", u);
        }
    }

    #[test]
    fn check_specifiers() {
        let read_allowlist = if cfg!(target_os = "windows") {
            vec![PathBuf::from("C:\\a")]
        } else {
            vec![PathBuf::from("/a")]
        };
        let mut perms = Permissions::from_options(&PermissionsOptions {
            allow_read: Some(read_allowlist),
            allow_net: Some(svec!["localhost"]),
            ..Default::default()
        })
        .unwrap();

        let mut fixtures = vec![
            (
                ModuleSpecifier::parse("http://localhost:4545/mod.ts").unwrap(),
                true,
            ),
            (
                ModuleSpecifier::parse("http://deno.land/x/mod.ts").unwrap(),
                false,
            ),
            (
                ModuleSpecifier::parse("data:text/plain,Hello%2C%20Deno!").unwrap(),
                true,
            ),
        ];

        if cfg!(target_os = "windows") {
            fixtures.push((ModuleSpecifier::parse("file:///C:/a/mod.ts").unwrap(), true));
            fixtures.push((
                ModuleSpecifier::parse("file:///C:/b/mod.ts").unwrap(),
                false,
            ));
        } else {
            fixtures.push((ModuleSpecifier::parse("file:///a/mod.ts").unwrap(), true));
            fixtures.push((ModuleSpecifier::parse("file:///b/mod.ts").unwrap(), false));
        }

        for (specifier, expected) in fixtures {
            assert_eq!(
                perms.check_specifier(&specifier).is_ok(),
                expected,
                "{}",
                specifier,
            );
        }
    }

    #[test]
    fn check_invalid_specifiers() {
        let mut perms = Permissions::allow_all();

        let mut test_cases = vec![];

        if cfg!(target_os = "windows") {
            test_cases.push("file://");
            test_cases.push("file:///");
        } else {
            test_cases.push("file://remotehost/");
        }

        for url in test_cases {
            assert!(perms
                .check_specifier(&ModuleSpecifier::parse(url).unwrap())
                .is_err());
        }
    }

    #[test]
    fn test_query() {
        let perms1 = Permissions::allow_all();
        let perms2 = Permissions {
            read: Permissions::new_read(&Some(vec![PathBuf::from("/foo")]), &None, false).unwrap(),
            write: Permissions::new_write(&Some(vec![PathBuf::from("/foo")]), &None, false)
                .unwrap(),
            ffi: Permissions::new_ffi(&Some(vec![PathBuf::from("/foo")]), &None, false).unwrap(),
            net: Permissions::new_net(&Some(svec!["127.0.0.1:8000"]), &None, false).unwrap(),
            env: Permissions::new_env(&Some(svec!["HOME"]), &None, false).unwrap(),
            sys: Permissions::new_sys(&Some(svec!["hostname"]), &None, false).unwrap(),
            run: Permissions::new_run(&Some(svec!["deno"]), &None, false).unwrap(),
            hrtime: Permissions::new_hrtime(false, false),
        };
        let perms3 = Permissions {
            read: Permissions::new_read(&None, &Some(vec![PathBuf::from("/foo")]), false).unwrap(),
            write: Permissions::new_write(&None, &Some(vec![PathBuf::from("/foo")]), false)
                .unwrap(),
            ffi: Permissions::new_ffi(&None, &Some(vec![PathBuf::from("/foo")]), false).unwrap(),
            net: Permissions::new_net(&None, &Some(svec!["127.0.0.1:8000"]), false).unwrap(),
            env: Permissions::new_env(&None, &Some(svec!["HOME"]), false).unwrap(),
            sys: Permissions::new_sys(&None, &Some(svec!["hostname"]), false).unwrap(),
            run: Permissions::new_run(&None, &Some(svec!["deno"]), false).unwrap(),
            hrtime: Permissions::new_hrtime(false, true),
        };
        let perms4 = Permissions {
            read: Permissions::new_read(&Some(vec![]), &Some(vec![PathBuf::from("/foo")]), false)
                .unwrap(),
            write: Permissions::new_write(&Some(vec![]), &Some(vec![PathBuf::from("/foo")]), false)
                .unwrap(),
            ffi: Permissions::new_ffi(&Some(vec![]), &Some(vec![PathBuf::from("/foo")]), false)
                .unwrap(),
            net: Permissions::new_net(&Some(vec![]), &Some(svec!["127.0.0.1:8000"]), false)
                .unwrap(),
            env: Permissions::new_env(&Some(vec![]), &Some(svec!["HOME"]), false).unwrap(),
            sys: Permissions::new_sys(&Some(vec![]), &Some(svec!["hostname"]), false).unwrap(),
            run: Permissions::new_run(&Some(vec![]), &Some(svec!["deno"]), false).unwrap(),
            hrtime: Permissions::new_hrtime(true, true),
        };
        #[rustfmt::skip]
    {
      assert_eq!(perms1.read.query(None), PermissionState::Granted);
      assert_eq!(perms1.read.query(Some(Path::new("/foo"))), PermissionState::Granted);
      assert_eq!(perms2.read.query(None), PermissionState::Prompt);
      assert_eq!(perms2.read.query(Some(Path::new("/foo"))), PermissionState::Granted);
      assert_eq!(perms2.read.query(Some(Path::new("/foo/bar"))), PermissionState::Granted);
      assert_eq!(perms3.read.query(None), PermissionState::Prompt);
      assert_eq!(perms3.read.query(Some(Path::new("/foo"))), PermissionState::Denied);
      assert_eq!(perms3.read.query(Some(Path::new("/foo/bar"))), PermissionState::Denied);
      assert_eq!(perms4.read.query(None), PermissionState::GrantedPartial);
      assert_eq!(perms4.read.query(Some(Path::new("/foo"))), PermissionState::Denied);
      assert_eq!(perms4.read.query(Some(Path::new("/foo/bar"))), PermissionState::Denied);
      assert_eq!(perms4.read.query(Some(Path::new("/bar"))), PermissionState::Granted);
      assert_eq!(perms1.write.query(None), PermissionState::Granted);
      assert_eq!(perms1.write.query(Some(Path::new("/foo"))), PermissionState::Granted);
      assert_eq!(perms2.write.query(None), PermissionState::Prompt);
      assert_eq!(perms2.write.query(Some(Path::new("/foo"))), PermissionState::Granted);
      assert_eq!(perms2.write.query(Some(Path::new("/foo/bar"))), PermissionState::Granted);
      assert_eq!(perms3.write.query(None), PermissionState::Prompt);
      assert_eq!(perms3.write.query(Some(Path::new("/foo"))), PermissionState::Denied);
      assert_eq!(perms3.write.query(Some(Path::new("/foo/bar"))), PermissionState::Denied);
      assert_eq!(perms4.write.query(None), PermissionState::GrantedPartial);
      assert_eq!(perms4.write.query(Some(Path::new("/foo"))), PermissionState::Denied);
      assert_eq!(perms4.write.query(Some(Path::new("/foo/bar"))), PermissionState::Denied);
      assert_eq!(perms4.write.query(Some(Path::new("/bar"))), PermissionState::Granted);
      assert_eq!(perms1.ffi.query(None), PermissionState::Granted);
      assert_eq!(perms1.ffi.query(Some(Path::new("/foo"))), PermissionState::Granted);
      assert_eq!(perms2.ffi.query(None), PermissionState::Prompt);
      assert_eq!(perms2.ffi.query(Some(Path::new("/foo"))), PermissionState::Granted);
      assert_eq!(perms2.ffi.query(Some(Path::new("/foo/bar"))), PermissionState::Granted);
      assert_eq!(perms3.ffi.query(None), PermissionState::Prompt);
      assert_eq!(perms3.ffi.query(Some(Path::new("/foo"))), PermissionState::Denied);
      assert_eq!(perms3.ffi.query(Some(Path::new("/foo/bar"))), PermissionState::Denied);
      assert_eq!(perms4.ffi.query(None), PermissionState::GrantedPartial);
      assert_eq!(perms4.ffi.query(Some(Path::new("/foo"))), PermissionState::Denied);
      assert_eq!(perms4.ffi.query(Some(Path::new("/foo/bar"))), PermissionState::Denied);
      assert_eq!(perms4.ffi.query(Some(Path::new("/bar"))), PermissionState::Granted);
      assert_eq!(perms1.net.query::<&str>(None), PermissionState::Granted);
      assert_eq!(perms1.net.query(Some(&("127.0.0.1", None))), PermissionState::Granted);
      assert_eq!(perms2.net.query::<&str>(None), PermissionState::Prompt);
      assert_eq!(perms2.net.query(Some(&("127.0.0.1", Some(8000)))), PermissionState::Granted);
      assert_eq!(perms3.net.query::<&str>(None), PermissionState::Prompt);
      assert_eq!(perms3.net.query(Some(&("127.0.0.1", Some(8000)))), PermissionState::Denied);
      assert_eq!(perms4.net.query::<&str>(None), PermissionState::GrantedPartial);
      assert_eq!(perms4.net.query(Some(&("127.0.0.1", Some(8000)))), PermissionState::Denied);
      assert_eq!(perms4.net.query(Some(&("192.168.0.1", Some(8000)))), PermissionState::Granted);
      assert_eq!(perms1.env.query(None), PermissionState::Granted);
      assert_eq!(perms1.env.query(Some("HOME")), PermissionState::Granted);
      assert_eq!(perms2.env.query(None), PermissionState::Prompt);
      assert_eq!(perms2.env.query(Some("HOME")), PermissionState::Granted);
      assert_eq!(perms3.env.query(None), PermissionState::Prompt);
      assert_eq!(perms3.env.query(Some("HOME")), PermissionState::Denied);
      assert_eq!(perms4.env.query(None), PermissionState::GrantedPartial);
      assert_eq!(perms4.env.query(Some("HOME")), PermissionState::Denied);
      assert_eq!(perms4.env.query(Some("AWAY")), PermissionState::Granted);
      assert_eq!(perms1.sys.query(None), PermissionState::Granted);
      assert_eq!(perms1.sys.query(Some("HOME")), PermissionState::Granted);
      assert_eq!(perms2.sys.query(None), PermissionState::Prompt);
      assert_eq!(perms2.sys.query(Some("hostname")), PermissionState::Granted);
      assert_eq!(perms3.sys.query(None), PermissionState::Prompt);
      assert_eq!(perms3.sys.query(Some("hostname")), PermissionState::Denied);
      assert_eq!(perms4.sys.query(None), PermissionState::GrantedPartial);
      assert_eq!(perms4.sys.query(Some("hostname")), PermissionState::Denied);
      assert_eq!(perms4.sys.query(Some("uid")), PermissionState::Granted);
      assert_eq!(perms1.run.query(None), PermissionState::Granted);
      assert_eq!(perms1.run.query(Some("deno")), PermissionState::Granted);
      assert_eq!(perms2.run.query(None), PermissionState::Prompt);
      assert_eq!(perms2.run.query(Some("deno")), PermissionState::Granted);
      assert_eq!(perms3.run.query(None), PermissionState::Prompt);
      assert_eq!(perms3.run.query(Some("deno")), PermissionState::Denied);
      assert_eq!(perms4.run.query(None), PermissionState::GrantedPartial);
      assert_eq!(perms4.run.query(Some("deno")), PermissionState::Denied);
      assert_eq!(perms4.run.query(Some("node")), PermissionState::Granted);
      assert_eq!(perms1.hrtime.query(), PermissionState::Granted);
      assert_eq!(perms2.hrtime.query(), PermissionState::Prompt);
      assert_eq!(perms3.hrtime.query(), PermissionState::Denied);
      assert_eq!(perms4.hrtime.query(), PermissionState::Denied);
    };
    }

    #[test]
    fn test_check() {
        let mut perms = Permissions {
            read: Permissions::new_read(&None, &None, true).unwrap(),
            write: Permissions::new_write(&None, &None, true).unwrap(),
            net: Permissions::new_net(&None, &None, true).unwrap(),
            env: Permissions::new_env(&None, &None, true).unwrap(),
            sys: Permissions::new_sys(&None, &None, true).unwrap(),
            run: Permissions::new_run(&None, &None, true).unwrap(),
            ffi: Permissions::new_ffi(&None, &None, true).unwrap(),
            hrtime: Permissions::new_hrtime(false, false),
        };

        assert!(perms.read.check(Path::new("/foo"), None).is_err());
        assert!(perms.read.check(Path::new("/bar"), None).is_err());

        assert!(perms.ffi.check(Path::new("/foo"), None).is_err());
        assert!(perms.ffi.check(Path::new("/bar"), None).is_err());

        assert!(perms.net.check(&("127.0.0.1", Some(8000)), None).is_err());
        assert!(perms.net.check(&("127.0.0.1", Some(8001)), None).is_err());
        assert!(perms.net.check(&("127.0.0.1", None), None).is_err());
        assert!(perms.net.check(&("deno.land", Some(8000)), None).is_err());
        assert!(perms.net.check(&("deno.land", None), None).is_err());

        assert!(perms.run.check("ls", None).is_err());

        assert!(perms.env.check("PATH").is_err());

        assert!(perms.env.check("hostname").is_err());
        assert!(perms.env.check("osRelease").is_err());

        assert!(perms.hrtime.check().is_err());
    }

    #[test]
    fn test_check_fail() {
        let mut perms = Permissions {
            read: Permissions::new_read(&None, &None, true).unwrap(),
            write: Permissions::new_write(&None, &None, true).unwrap(),
            net: Permissions::new_net(&None, &None, true).unwrap(),
            env: Permissions::new_env(&None, &None, true).unwrap(),
            sys: Permissions::new_sys(&None, &None, true).unwrap(),
            run: Permissions::new_run(&None, &None, true).unwrap(),
            ffi: Permissions::new_ffi(&None, &None, true).unwrap(),
            hrtime: Permissions::new_hrtime(false, false),
        };

        assert!(perms.read.check(Path::new("/foo"), None).is_err());

        assert!(perms.write.check(Path::new("/foo"), None).is_err());

        assert!(perms.ffi.check(Path::new("/foo"), None).is_err());

        assert!(perms.net.check(&("127.0.0.1", Some(8000)), None).is_err());

        assert!(perms.run.check("cat", None).is_err());
        assert!(perms.run.check("ls", None).is_err());

        assert!(perms.env.check("HOME").is_err());

        assert!(perms.sys.check("hostname", None).is_err());
        assert!(perms.sys.check("osRelease", None).is_err());

        assert!(perms.hrtime.check().is_err());
        assert!(perms.hrtime.check().is_err());
    }

    #[test]
    fn test_deserialize_child_permissions_arg() {
        assert_eq!(
            ChildPermissionsArg::inherit(),
            ChildPermissionsArg {
                env: ChildUnaryPermissionArg::Inherit,
                hrtime: ChildUnitPermissionArg::Inherit,
                net: ChildUnaryPermissionArg::Inherit,
                ffi: ChildUnaryPermissionArg::Inherit,
                read: ChildUnaryPermissionArg::Inherit,
                run: ChildUnaryPermissionArg::Inherit,
                sys: ChildUnaryPermissionArg::Inherit,
                write: ChildUnaryPermissionArg::Inherit,
            }
        );
        assert_eq!(
            ChildPermissionsArg::none(),
            ChildPermissionsArg {
                env: ChildUnaryPermissionArg::NotGranted,
                hrtime: ChildUnitPermissionArg::NotGranted,
                net: ChildUnaryPermissionArg::NotGranted,
                ffi: ChildUnaryPermissionArg::NotGranted,
                read: ChildUnaryPermissionArg::NotGranted,
                run: ChildUnaryPermissionArg::NotGranted,
                sys: ChildUnaryPermissionArg::NotGranted,
                write: ChildUnaryPermissionArg::NotGranted,
            }
        );
        assert_eq!(
            serde_json::from_value::<ChildPermissionsArg>(json!("inherit")).unwrap(),
            ChildPermissionsArg::inherit()
        );
        assert_eq!(
            serde_json::from_value::<ChildPermissionsArg>(json!("none")).unwrap(),
            ChildPermissionsArg::none()
        );
        assert_eq!(
            serde_json::from_value::<ChildPermissionsArg>(json!({})).unwrap(),
            ChildPermissionsArg::none()
        );
        assert_eq!(
            serde_json::from_value::<ChildPermissionsArg>(json!({
              "env": ["foo", "bar"],
            }))
            .unwrap(),
            ChildPermissionsArg {
                env: ChildUnaryPermissionArg::GrantedList(svec!["foo", "bar"]),
                ..ChildPermissionsArg::none()
            }
        );
        assert_eq!(
            serde_json::from_value::<ChildPermissionsArg>(json!({
              "hrtime": true,
            }))
            .unwrap(),
            ChildPermissionsArg {
                hrtime: ChildUnitPermissionArg::Granted,
                ..ChildPermissionsArg::none()
            }
        );
        assert_eq!(
            serde_json::from_value::<ChildPermissionsArg>(json!({
              "hrtime": false,
            }))
            .unwrap(),
            ChildPermissionsArg {
                hrtime: ChildUnitPermissionArg::NotGranted,
                ..ChildPermissionsArg::none()
            }
        );
        assert_eq!(
            serde_json::from_value::<ChildPermissionsArg>(json!({
              "env": true,
              "net": true,
              "ffi": true,
              "read": true,
              "run": true,
              "sys": true,
              "write": true,
            }))
            .unwrap(),
            ChildPermissionsArg {
                env: ChildUnaryPermissionArg::Granted,
                net: ChildUnaryPermissionArg::Granted,
                ffi: ChildUnaryPermissionArg::Granted,
                read: ChildUnaryPermissionArg::Granted,
                run: ChildUnaryPermissionArg::Granted,
                sys: ChildUnaryPermissionArg::Granted,
                write: ChildUnaryPermissionArg::Granted,
                ..ChildPermissionsArg::none()
            }
        );
        assert_eq!(
            serde_json::from_value::<ChildPermissionsArg>(json!({
              "env": false,
              "net": false,
              "ffi": false,
              "read": false,
              "run": false,
              "sys": false,
              "write": false,
            }))
            .unwrap(),
            ChildPermissionsArg {
                env: ChildUnaryPermissionArg::NotGranted,
                net: ChildUnaryPermissionArg::NotGranted,
                ffi: ChildUnaryPermissionArg::NotGranted,
                read: ChildUnaryPermissionArg::NotGranted,
                run: ChildUnaryPermissionArg::NotGranted,
                sys: ChildUnaryPermissionArg::NotGranted,
                write: ChildUnaryPermissionArg::NotGranted,
                ..ChildPermissionsArg::none()
            }
        );
        assert_eq!(
            serde_json::from_value::<ChildPermissionsArg>(json!({
              "env": ["foo", "bar"],
              "net": ["foo", "bar:8000"],
              "ffi": ["foo", "file:///bar/baz"],
              "read": ["foo", "file:///bar/baz"],
              "run": ["foo", "file:///bar/baz", "./qux"],
              "sys": ["hostname", "osRelease"],
              "write": ["foo", "file:///bar/baz"],
            }))
            .unwrap(),
            ChildPermissionsArg {
                env: ChildUnaryPermissionArg::GrantedList(svec!["foo", "bar"]),
                net: ChildUnaryPermissionArg::GrantedList(svec!["foo", "bar:8000"]),
                ffi: ChildUnaryPermissionArg::GrantedList(svec!["foo", "file:///bar/baz"]),
                read: ChildUnaryPermissionArg::GrantedList(svec!["foo", "file:///bar/baz"]),
                run: ChildUnaryPermissionArg::GrantedList(svec!["foo", "file:///bar/baz", "./qux"]),
                sys: ChildUnaryPermissionArg::GrantedList(svec!["hostname", "osRelease"]),
                write: ChildUnaryPermissionArg::GrantedList(svec!["foo", "file:///bar/baz"]),
                ..ChildPermissionsArg::none()
            }
        );
    }

    #[test]
    fn test_create_child_permissions() {
        let mut main_perms = Permissions {
            env: Permissions::new_env(&Some(vec![]), &None, false).unwrap(),
            hrtime: Permissions::new_hrtime(true, false),
            net: Permissions::new_net(&Some(svec!["foo", "bar"]), &None, false).unwrap(),
            ..Default::default()
        };
        assert_eq!(
            create_child_permissions(
                &mut main_perms.clone(),
                ChildPermissionsArg {
                    env: ChildUnaryPermissionArg::Inherit,
                    hrtime: ChildUnitPermissionArg::NotGranted,
                    net: ChildUnaryPermissionArg::GrantedList(svec!["foo"]),
                    ffi: ChildUnaryPermissionArg::NotGranted,
                    ..ChildPermissionsArg::none()
                }
            )
            .unwrap(),
            Permissions {
                env: Permissions::new_env(&Some(vec![]), &None, false).unwrap(),
                net: Permissions::new_net(&Some(svec!["foo"]), &None, false).unwrap(),
                ..Default::default()
            }
        );
        assert!(create_child_permissions(
            &mut main_perms.clone(),
            ChildPermissionsArg {
                net: ChildUnaryPermissionArg::Granted,
                ..ChildPermissionsArg::none()
            }
        )
        .is_err());
        assert!(create_child_permissions(
            &mut main_perms.clone(),
            ChildPermissionsArg {
                net: ChildUnaryPermissionArg::GrantedList(svec!["foo", "bar", "baz"]),
                ..ChildPermissionsArg::none()
            }
        )
        .is_err());
        assert!(create_child_permissions(
            &mut main_perms,
            ChildPermissionsArg {
                ffi: ChildUnaryPermissionArg::GrantedList(svec!["foo"]),
                ..ChildPermissionsArg::none()
            }
        )
        .is_err());
    }

    #[test]
    fn test_create_child_permissions_with_inherited_denied_list() {
        let mut main_perms = Permissions::from_options(&PermissionsOptions {
            prompt: true,
            ..Default::default()
        })
        .unwrap();
        assert!(main_perms.write.check(&PathBuf::from("foo"), None).is_err());
        let worker_perms =
            create_child_permissions(&mut main_perms.clone(), ChildPermissionsArg::none()).unwrap();
        assert_eq!(
            worker_perms.write.flag_denied_list,
            main_perms.write.flag_denied_list
        );
    }

    #[test]
    fn test_handle_empty_value() {
        assert!(Permissions::new_read(&Some(vec![PathBuf::new()]), &None, false).is_err());
        assert!(Permissions::new_env(&Some(vec![String::new()]), &None, false).is_err());
        assert!(Permissions::new_sys(&Some(vec![String::new()]), &None, false).is_err());
        assert!(Permissions::new_run(&Some(vec![String::new()]), &None, false).is_err());
        assert!(Permissions::new_ffi(&Some(vec![PathBuf::new()]), &None, false).is_err());
        assert!(Permissions::new_net(&Some(svec![String::new()]), &None, false).is_err());
        assert!(Permissions::new_write(&Some(vec![PathBuf::new()]), &None, false).is_err());
    }
}
