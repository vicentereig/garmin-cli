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
    /// Health metrics commands (including weight)
    Health {
        #[command(subcommand)]
        command: HealthCommands,
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
    /// Get daily step counts
    Steps {
        /// Number of days to show (default: 10)
        #[arg(long, default_value = "10")]
        days: u32,
    },
    /// Get calorie data
    Calories {
        /// Number of days to show (default: 10)
        #[arg(long, default_value = "10")]
        days: u32,
    },
    /// Get VO2 max and performance metrics
    Vo2max {
        /// Date (YYYY-MM-DD), defaults to today
        #[arg(short, long)]
        date: Option<String>,
    },
    /// Get training readiness score
    TrainingReadiness {
        /// Date (YYYY-MM-DD), defaults to today
        #[arg(short, long)]
        date: Option<String>,
    },
    /// Get training status
    TrainingStatus {
        /// Date (YYYY-MM-DD), defaults to today
        #[arg(short, long)]
        date: Option<String>,
    },
    /// Get HRV (heart rate variability) data
    Hrv {
        /// Date (YYYY-MM-DD), defaults to today
        #[arg(short, long)]
        date: Option<String>,
    },
    /// Get fitness age
    FitnessAge {
        /// Date (YYYY-MM-DD), defaults to today
        #[arg(short, long)]
        date: Option<String>,
    },
    /// List weight entries
    Weight {
        /// Start date (YYYY-MM-DD)
        #[arg(long)]
        from: Option<String>,
        /// End date (YYYY-MM-DD)
        #[arg(long)]
        to: Option<String>,
    },
    /// Add weight entry
    WeightAdd {
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
            HealthCommands::Steps { days } => {
                commands::steps(Some(days), cli.profile).await
            }
            HealthCommands::Calories { days } => {
                commands::calories(Some(days), cli.profile).await
            }
            HealthCommands::Vo2max { date } => {
                commands::vo2max(date, cli.profile).await
            }
            HealthCommands::TrainingReadiness { date } => {
                commands::training_readiness(date, cli.profile).await
            }
            HealthCommands::TrainingStatus { date } => {
                commands::training_status(date, cli.profile).await
            }
            HealthCommands::Hrv { date } => {
                commands::hrv(date, cli.profile).await
            }
            HealthCommands::FitnessAge { date } => {
                commands::fitness_age(date, cli.profile).await
            }
            HealthCommands::Weight { from, to } => {
                commands::list_weight(from, to, cli.profile).await
            }
            HealthCommands::WeightAdd { weight, unit } => {
                commands::add_weight(weight, &unit, cli.profile).await
            }
        },
        Commands::Devices { command } => match command {
            DeviceCommands::List => {
                commands::list_devices(cli.profile).await
            }
            DeviceCommands::Get { id } => {
                commands::get_device(&id, cli.profile).await
            }
        },
        Commands::Profile { command } => match command {
            ProfileCommands::Show => {
                commands::show_profile(cli.profile).await
            }
            ProfileCommands::Settings => {
                commands::show_settings(cli.profile).await
            }
        },
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }

    Ok(())
}
