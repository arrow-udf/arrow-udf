// all paths are relative to package.json when run with `npm run build`
require("esbuild")
  .build({
    entryPoints: ["./dist/index.js"],
    bundle: true,
    minify: true,
    keepNames: false,
    sourcemap: false,
    target: "es2020",
    outfile: "./bundled/bundled.table.js",
    format: "iife",
    preserveSymlinks: true,
    plugins: [],
    treeShaking: true,
    globalName: 'z',
    footer: {
      js: 'return z.createAsyncIterable();',
    },
    banner: {
      js: "//This code was generated using esbuild",
    },
  })
  .catch(() => process.exit(1));
