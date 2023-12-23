fn main() {
    if std::env::var("CARGO_CFG_TARGET_ARCH").unwrap() == "wasm32" {
        wlr_libpy::bld_cfg::configure_static_libs()
            .unwrap()
            .emit_link_flags();
    }
}
