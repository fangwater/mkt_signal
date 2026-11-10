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
            .compile("cn_futures_ctp_bridge");
        println!("cargo:rustc-link-lib=dl");
    }
    println!("cargo:rerun-if-env-changed=YIDA_SDK_DIR");
    println!("cargo:rerun-if-changed=cpp/yida_bridge.cpp");
    println!("cargo:rerun-if-changed=cpp/yida_bridge.h");
    #[cfg(feature = "yida-sdk")]
    {
        let sdk = std::path::PathBuf::from(
            std::env::var_os("YIDA_SDK_DIR")
                .expect("yida-sdk requires YIDA_SDK_DIR with native 1.502.53.182 headers"),
        );
        for name in ["ydApi.h", "ydDataStruct.h", "ydDataType.h"] {
            assert!(sdk.join(name).is_file(), "missing {} in YIDA_SDK_DIR", name);
            println!("cargo:rerun-if-changed={}", sdk.join(name).display());
        }
        cc::Build::new()
            .cpp(true)
            .std("c++17")
            .flag("-isystem")
            .flag(sdk.to_str().expect("SDK path must be UTF-8"))
            .include("cpp")
            .file("cpp/yida_bridge.cpp")
            .warnings(true)
            .compile("cn_futures_yida_bridge");
        println!("cargo:rustc-link-lib=dl");
        println!("cargo:rustc-link-lib=pthread");
    }
}
