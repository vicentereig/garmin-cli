use clap::{Parser, Subcommand};
use garmin_cli::cli::commands;

#[derive(Parser)]
#[command(name = "garmin")]
#[command(author, version, about = "CLI for Garmin Connect API", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Output format
    #[arg(short, long, global = true, default_value = "table")]
    format: OutputFormat,

    /// Profile to use
    #[arg(short, long, global = true, env = "GARMIN_PROFILE")]
    profile: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum)]
enum OutputFormat {
    #[default]
    Table,
    Json,
    Csv,
}

#[derive(Subcommand)]
enum Commands {
    /// Authentication commands
    Auth {
        #[command(subcommand)]
        command: AuthCommands,
    },
    /// Activity commands
    Activities {
        #[command(subcommand)]
        command: ActivityCommands,
    },
    /// Health metrics commands
    Health {
        #[command(subcommand)]
        command: HealthCommands,
    },
    /// Weight and body composition commands
    Weight {
        #[command(subcommand)]
        command: WeightCommands,
    },
    /// Device commands
    Devices {
        #[command(subcommand)]
        command: DeviceCommands,
    },
    /// User profile commands
    Profile {
        #[command(subcommand)]
        command: ProfileCommands,
    },
}

#[derive(Subcommand)]
enum AuthCommands {
    /// Login to Garmin Connect
    Login {
        /// Email address
        #[arg(short, long, env = "GARMIN_EMAIL")]
        email: Option<String>,
    },
    /// Logout and clear credentials
    Logout,
    /// Show authentication status
    Status,
}

#[derive(Subcommand)]
enum ActivityCommands {
    /// List activities
    List {
        /// Number of activities to show
        #[arg(short, long, default_value = "20")]
        limit: u32,
        /// Starting offset
        #[arg(short, long, default_value = "0")]
        start: u32,
    },
    /// Get activity details
    Get {
        /// Activity ID
        id: u64,
    },
    /// Download activity file
    Download {
        /// Activity ID
        id: u64,
        /// File format (fit, gpx, tcx, kml)
        #[arg(short = 't', long = "type", default_value = "fit")]
        file_type: String,
        /// Output file path
        #[arg(short, long)]
        output: Option<String>,
    },
    /// Upload activity file
    Upload {
        /// File path to upload
        file: String,
    },
}

#[derive(Subcommand)]
enum HealthCommands {
    /// Get daily summary
    Summary {
        /// Date (YYYY-MM-DD), defaults to today
        #[arg(short, long)]
        date: Option<String>,
    },
    /// Get sleep data
    Sleep {
        /// Date (YYYY-MM-DD), defaults to today
        #[arg(short, long)]
        date: Option<String>,
        /// Number of days to show (overrides date)
        #[arg(long)]
        days: Option<u32>,
    },
    /// Get stress data
    Stress {
        /// Date (YYYY-MM-DD), defaults to today
        #[arg(short, long)]
        date: Option<String>,
        /// Number of days to show (overrides date)
        #[arg(long)]
        days: Option<u32>,
    },
    /// Get body battery data
    BodyBattery {
        /// Date (YYYY-MM-DD), defaults to today
        #[arg(short, long)]
        date: Option<String>,
    },
    /// Get heart rate data
    HeartRate {
        /// Date (YYYY-MM-DD), defaults to today
        #[arg(short, long)]
        date: Option<String>,
    },
}

#[derive(Subcommand)]
enum WeightCommands {
    /// List weight entries
    List {
        /// Start date (YYYY-MM-DD)
        #[arg(long)]
        from: Option<String>,
        /// End date (YYYY-MM-DD)
        #[arg(long)]
        to: Option<String>,
    },
    /// Add weight entry
    Add {
        /// Weight value
        weight: f64,
        /// Unit (kg or lbs)
        #[arg(short, long, default_value = "kg")]
        unit: String,
    },
}

#[derive(Subcommand)]
enum DeviceCommands {
    /// List registered devices
    List,
    /// Get device info
    Get {
        /// Device ID
        id: String,
    },
}

#[derive(Subcommand)]
enum ProfileCommands {
    /// Show user profile
    Show,
    /// Show user settings
    Settings,
}

#[tokio::main]
async fn main() -> garmin_cli::Result<()> {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Auth { command } => match command {
            AuthCommands::Login { email } => {
                commands::login(email, cli.profile).await
            }
            AuthCommands::Logout => {
                commands::logout(cli.profile).await
            }
            AuthCommands::Status => {
                commands::status(cli.profile).await
            }
        },
        Commands::Activities { command } => match command {
            ActivityCommands::List { limit, start } => {
                commands::list_activities(limit, start, cli.profile).await
            }
            ActivityCommands::Get { id } => {
                commands::get_activity(id, cli.profile).await
            }
            ActivityCommands::Download { id, file_type, output } => {
                commands::download_activity(id, &file_type, output, cli.profile).await
            }
            ActivityCommands::Upload { file } => {
                commands::upload_activity(&file, cli.profile).await
            }
        },
        Commands::Health { command } => match command {
            HealthCommands::Summary { date } => {
                commands::summary(date, cli.profile).await
            }
            HealthCommands::Sleep { date, days } => {
                if let Some(d) = days {
                    commands::sleep_range(d, cli.profile).await
                } else {
                    commands::sleep(date, cli.profile).await
                }
            }
            HealthCommands::Stress { date, days } => {
                if let Some(d) = days {
                    commands::stress_range(d, cli.profile).await
                } else {
                    commands::stress(date, cli.profile).await
                }
            }
            HealthCommands::BodyBattery { date } => {
                commands::body_battery(date, cli.profile).await
            }
            HealthCommands::HeartRate { date } => {
                commands::heart_rate(date, cli.profile).await
            }
        },
        Commands::Weight { command } => match command {
            WeightCommands::List { from, to } => {
                println!("Listing weight from {:?} to {:?}", from, to);
                // TODO: Implement weight list
                Ok(())
            }
            WeightCommands::Add { weight, unit } => {
                println!("Adding weight {} {}", weight, unit);
                // TODO: Implement weight add
                Ok(())
            }
        },
        Commands::Devices { command } => match command {
            DeviceCommands::List => {
                println!("Listing devices...");
                // TODO: Implement device list
                Ok(())
            }
            DeviceCommands::Get { id } => {
                println!("Getting device {}", id);
                // TODO: Implement device get
                Ok(())
            }
        },
        Commands::Profile { command } => match command {
            ProfileCommands::Show => {
                println!("Showing profile...");
                // TODO: Implement profile show
                Ok(())
            }
            ProfileCommands::Settings => {
                println!("Showing settings...");
                // TODO: Implement settings show
                Ok(())
            }
        },
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }

    Ok(())
}
