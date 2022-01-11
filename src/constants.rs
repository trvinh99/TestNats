pub const POSITIVE: &'static str = "positive";
pub const NEGATIVE: &'static str = "negative";

pub const LIMIT_STEP: i64 = 20_000_000_000i64;
pub const ONE_SEC: i64 = 1_000_000_000i64;
pub const ONE_MIL_SEC: i64 = 1_000_000i64;
pub const ONE_NAN_SEC: i64 = 1i64;
pub const ONE_GB: u64 = 1_000_000_000u64;

lazy_static::lazy_static! {
    pub static ref CONFIGURATION_DIR: String =
        std::env::var("CONFIGURATION_DIR").unwrap_or("/etc/lexhub/configs".to_owned());
    pub static ref CREDENTIALS_DIR: String =
        std::env::var("CREDENTIALS_DIR").unwrap_or("/etc/lexhub/creds".to_owned());
    pub static ref DATABASE_DIR: String =
        std::env::var("DATABASE_DIR").unwrap_or("/etc/lexhub/database".to_owned());
    pub static ref DOCUMENTS_DIR: String =
        std::env::var("DOCUMENTS_DIR").unwrap_or("/etc/lexhub/docs".to_owned());
    pub static ref RECORD_DIR: String =
        std::env::var("RECORD_DIR").unwrap_or("/home/lexhub/database/record".to_owned());
    pub static ref RECORD_FRAME_DIR: String =
        std::env::var("RECORD_FRAME_DIR").unwrap_or("/data/lexhub/record".to_owned());
    pub static ref NATS_URL: String = std::env::var("NATS_URL")
        .unwrap_or("https://hub-svc.lexray.com/box/hubs/setting".to_owned());
    pub static ref J2C_HOST: String = std::env::var("J2C_HOST").unwrap_or("127.0.0.1".to_owned());
}
