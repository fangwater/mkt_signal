limit_except GET {
    deny all;
}

proxy_pass https://api.bitget.com;
proxy_http_version 1.1;
proxy_set_header Host api.bitget.com;
proxy_set_header Connection "";
proxy_set_header Accept-Encoding "";
proxy_set_header Content-Length "";
proxy_pass_request_body off;

proxy_set_header ACCESS-KEY "";
proxy_set_header ACCESS-SIGN "";
proxy_set_header ACCESS-TIMESTAMP "";
proxy_set_header ACCESS-PASSPHRASE "";

proxy_ssl_server_name on;
proxy_ssl_name api.bitget.com;

proxy_connect_timeout 3s;
proxy_read_timeout 5s;
proxy_send_timeout 5s;
proxy_next_upstream off;

proxy_cache bitget_public;
proxy_cache_key "bitget|$request_method|$uri|$args";
proxy_cache_lock on;
proxy_cache_lock_timeout 10s;
proxy_cache_lock_age 10s;
proxy_cache_use_stale off;
proxy_no_cache $bitget_public_skip_store;

proxy_ignore_headers Set-Cookie;
proxy_hide_header Set-Cookie;
add_header X-Public-API-Cache $upstream_cache_status always;
