use std::fmt;
use std::process;
use std::str;

use std::env::Args;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub struct ConfigError {
    msg: String,
}

impl ConfigError {
    /// Create a new error.
    pub fn new<T>(msg: T) -> Self
    where
        T: ToString,
    {
        Self {
            msg: msg.to_string(),
        }
    }
}

impl Error for ConfigError {}

impl Display for ConfigError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        f.write_str(&self.msg)
    }
}

/// Builder for the Lexray Hub configuration.
pub struct ConfigBuilder {
    nats_url: String,
    redundancy: usize,
    publisher_actors: usize,
    max_msg: usize,
    max_cams: usize,
    fps: usize,
}

impl ConfigBuilder {
    /// Create a new configuration builder.
    fn new() -> Self {
        Self {
            nats_url: String::from("dev.lexray.com:60064"),
            redundancy: 1,
            publisher_actors: 5,
            max_msg: 300,
            max_cams: 24,
            fps: 5,
        }
    }

    pub fn nats_url(&mut self, url: String) -> &mut Self {
        self.nats_url = url;
        self
    }

    pub fn redundancy(&mut self, redundancy: usize) -> &mut Self {
        self.redundancy = redundancy;
        self
    }

    pub fn publisher_actors(&mut self, actors: usize) -> &mut Self {
        self.publisher_actors = actors;
        self
    }

    pub fn max_msg(&mut self, max_msg: usize) -> &mut Self {
        self.max_msg = max_msg;
        self
    }

    pub fn max_cams(&mut self, max_cams: usize) -> &mut Self {
        self.max_cams = max_cams;
        self
    }

    pub fn fps(&mut self, fps: usize) -> &mut Self {
        self.fps = fps;
        self
    }

    /// Build the configuration.
    pub fn build(self) -> Result<Config, ConfigError> {
        let config = Config {
            nats_url: self.nats_url.clone(),
            redundancy: self.redundancy,
            publisher_actors: self.publisher_actors,
            max_msg: self.max_msg,
            max_cams: self.max_cams,
            fps: self.fps,
        };

        Ok(config)
    }
}

/// Builder for application configuration.
pub struct ConfigParser {
    nats_url: String,
    redundancy: usize,
    publisher_actors: usize,
    max_msg: usize,
    max_cams: usize,
    fps: usize,
}

impl ConfigParser {
    /// Create a new application configuration builder.
    pub fn new() -> Self {
        Self {
            nats_url: String::from("dev.lexray.com:60064"),
            redundancy: 1,
            publisher_actors: 5,
            max_msg: 300,
            max_cams: 24,
            fps: 5,
        }
    }

    /// Build application configuration.
    fn build(self) -> Result<Config, ConfigError> {
        let mut config_builder = Config::builder();

        config_builder
            .nats_url(self.nats_url)
            .redundancy(self.redundancy)
            .publisher_actors(self.publisher_actors)
            .max_msg(self.max_msg)
            .max_cams(self.max_cams)
            .fps(self.fps);

        let config = config_builder.build()?;

        Ok(config)
    }

    /// Parse given command line arguments.
    fn parse(mut self, mut args: Args) -> Result<Self, ConfigError> {
        args.next();

        while let Some(ref arg) = args.next() {
            match arg as &str {
                "-h" | "--help" => usage(1),

                arg => {
                    if arg.starts_with("--nats-url=") {
                        self.nats_url(arg)?;
                    } else if arg.starts_with("--redundancy=") {
                        self.redundancy(arg)?;
                    } else if arg.starts_with("--publisher-actors=") {
                        self.publisher_actors(arg)?;
                    } else if arg.starts_with("--max-msg=") {
                        self.max_msg(arg)?;
                    } else if arg.starts_with("--max-cams=") {
                        self.max_cams(arg)?;
                    } else if arg.starts_with("--fps") {
                        self.fps(arg)?;
                    } else {
                        return Err(ConfigError::new(format!("unknown argument: \"{}\"", arg)));
                    }
                }
            }
        }

        Ok(self)
    }

    fn nats_url(&mut self, arg: &str) -> Result<(), ConfigError> {
        let nats_url = &arg["--nats-url=".len()..];

        self.nats_url = nats_url.into();

        Ok(())
    }

    fn publisher_actors(&mut self, arg: &str) -> Result<(), ConfigError> {
        let actors = &arg["--publisher-actors=".len()..];

        self.publisher_actors = actors.parse().map_err(|_| {
            ConfigError::new(format!("invalid value given for {}, number expected", arg))
        })?;

        Ok(())
    }

    fn redundancy(&mut self, arg: &str) -> Result<(), ConfigError> {
        let redundancy = &arg["--redundancy=".len()..];

        self.redundancy = redundancy.parse().map_err(|_| {
            ConfigError::new(format!("invalid value given for {}, number expected", arg))
        })?;

        Ok(())
    }

    fn max_msg(&mut self, arg: &str) -> Result<(), ConfigError> {
        let max_msg = &arg["--max-msg=".len()..];

        self.max_msg = max_msg.parse().map_err(|_| {
            ConfigError::new(format!("invalid value given for {}, number expected", arg))
        })?;

        Ok(())
    }

    fn max_cams(&mut self, arg: &str) -> Result<(), ConfigError> {
        let max_cams = &arg["--max-cams=".len()..];

        self.max_cams = max_cams.parse().map_err(|_| {
            ConfigError::new(format!("invalid value given for {}, number expected", arg))
        })?;

        Ok(())
    }

    fn fps(&mut self, arg: &str) -> Result<(), ConfigError> {
        let fps = &arg["--fps=".len()..];

        self.fps = fps.parse().map_err(|_| {
            ConfigError::new(format!("invalid value given for {}, number expected", arg))
        })?;

        Ok(())
    }
}

/// Lexray Hub configuration.
pub struct Config {
    nats_url: String,
    redundancy: usize,
    publisher_actors: usize,
    max_msg: usize,
    max_cams: usize,
    fps: usize,
}

impl Config {
    /// Get a new client configuration builder.
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::new()
    }

    /// Create a new application configuration. The methods reads all command line arguments and
    /// loads the configuration file.
    pub fn from_args(args: Args) -> Result<Self, ConfigError> {
        ConfigParser::new().parse(args)?.build()
    }

    pub fn get_nats_url(&self) -> String {
        self.nats_url.clone()
    }

    pub fn get_redundancy(&self) -> usize {
        self.redundancy
    }

    pub fn get_publisher_actors(&self) -> usize {
        self.publisher_actors
    }

    pub fn get_max_msg(&self) -> usize {
        self.max_msg
    }

    pub fn get_max_cams(&self) -> usize {
        self.max_cams
    }

    pub fn get_fps(&self) -> usize {
        self.fps
    }
}

/// Print usage and exit the process with a given exit code.
#[doc(hidden)]
pub fn usage(exit_code: i32) -> ! {
    println!("USAGE: [OPTIONS]\n");
    println!("OPTIONS:\n");

    println!("    --nats-url=url          NATS url");
    println!("    --redundancy=           test redundancy");
    println!("    --publisher-actors=n    number of publisher actors");
    println!("    --max-msg=n             number of messages");
    println!("    --max-cams=n            number of cameras");
    println!("    --fps=n                 change number of frames per second\n");
    println!();

    process::exit(exit_code);
}
