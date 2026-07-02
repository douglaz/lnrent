# lnrent buyer web

Build the wasm package from the repo root:

```sh
wasm-pack build --target web clients/web
```

That writes `clients/web/pkg/`. Serve the files over a real static HTTP server with `pkg/` mounted next
to `index.html`, because wasm module loading needs normal HTTP fetches and the correct MIME type. Do
not use `file://`.

One simple local option is:

```sh
rm -rf /tmp/lnrent-buyer-web
mkdir -p /tmp/lnrent-buyer-web
cp -R clients/web/static/. /tmp/lnrent-buyer-web/
cp -R clients/web/pkg /tmp/lnrent-buyer-web/pkg
python3 -m http.server 8080 --directory /tmp/lnrent-buyer-web
```

Then open `http://127.0.0.1:8080/`.
