fn main() {
    println!("cargo:rerun-if-env-changed=CTP_SDK_DIR");
    println!("cargo:rerun-if-changed=cpp/ctp_bridge.cpp");
    println!("cargo:rerun-if-changed=cpp/ctp_bridge.h");
    #[cfg(feature = "ctp-sdk")]
    {
        let sdk = std::env::var_os("CTP_SDK_DIR").expect(
            "ctp-sdk requires CTP_SDK_DIR containing the matching official CTP 6.7.11 headers",
        );
        let sdk = std::path::PathBuf::from(sdk);
        for name in [
            "ThostFtdcTraderApi.h",
            "ThostFtdcUserApiStruct.h",
            "ThostFtdcUserApiDataType.h",
        ] {
            assert!(sdk.join(name).is_file(), "missing {} in CTP_SDK_DIR", name);
            println!("cargo:rerun-if-changed={}", sdk.join(name).display());
        }
        cc::Build::new()
            .cpp(true)
            .std("c++17")
            .flag("-isystem")
            .flag(sdk.to_str().expect("SDK path must be UTF-8"))
            .include("cpp")
            .file("cpp/ctp_bridge.cpp")
            .warnings(true)
            .compile("native_futures_ctp_bridge");
        println!("cargo:rustc-link-lib=dl");
    }
}
