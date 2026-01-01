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
    /// Sync data to local database
    Sync {
        #[command(subcommand)]
        command: SyncCommands,
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
        /// Number of days to show (overrides date)
        #[arg(long)]
        days: Option<u32>,
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
        /// Number of days to show (overrides date)
        #[arg(long)]
        days: Option<u32>,
    },
    /// Get training status
    TrainingStatus {
        /// Date (YYYY-MM-DD), defaults to today
        #[arg(short, long)]
        date: Option<String>,
        /// Number of days to show (overrides date)
        #[arg(long)]
        days: Option<u32>,
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
    /// Get lactate threshold
    LactateThreshold {
        /// Number of days to show (default: 90)
        #[arg(long, default_value = "90")]
        days: u32,
    },
    /// Get race predictions
    RacePredictions {
        /// Date (YYYY-MM-DD), defaults to today
        #[arg(short, long)]
        date: Option<String>,
    },
    /// Get endurance score
    EnduranceScore {
        /// Number of days to show (default: 30)
        #[arg(long, default_value = "30")]
        days: u32,
    },
    /// Get hill score
    HillScore {
        /// Number of days to show (default: 30)
        #[arg(long, default_value = "30")]
        days: u32,
    },
    /// Get SpO2 (blood oxygen) data
    Spo2 {
        /// Date (YYYY-MM-DD), defaults to today
        #[arg(short, long)]
        date: Option<String>,
    },
    /// Get respiration data
    Respiration {
        /// Date (YYYY-MM-DD), defaults to today
        #[arg(short, long)]
        date: Option<String>,
    },
    /// Get intensity minutes
    IntensityMinutes {
        /// Date (YYYY-MM-DD), defaults to today
        #[arg(short, long)]
        date: Option<String>,
    },
    /// Get blood pressure data
    BloodPressure {
        /// Start date (YYYY-MM-DD)
        #[arg(long)]
        from: Option<String>,
        /// End date (YYYY-MM-DD)
        #[arg(long)]
        to: Option<String>,
    },
    /// Get hydration data
    Hydration {
        /// Date (YYYY-MM-DD), defaults to today
        #[arg(short, long)]
        date: Option<String>,
    },
    /// Get personal records
    PersonalRecords,
    /// Get performance summary (all performance metrics)
    PerformanceSummary {
        /// Date (YYYY-MM-DD), defaults to today
        #[arg(short, long)]
        date: Option<String>,
    },
    /// Get health insights (sleep/stress correlations)
    Insights {
        /// Number of days to analyze (default: 28)
        #[arg(long, default_value = "28")]
        days: u32,
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
    /// Show device history from synced activities
    History {
        /// Database file path
        #[arg(long)]
        db: Option<String>,
    },
}

#[derive(Subcommand)]
enum ProfileCommands {
    /// Show user profile
    Show,
    /// Show user settings
    Settings,
}

#[derive(Subcommand)]
enum SyncCommands {
    /// Run sync operation
    Run {
        /// Database file path
        #[arg(long)]
        db: Option<String>,
        /// Sync activities only
        #[arg(long)]
        activities: bool,
        /// Sync health data only
        #[arg(long)]
        health: bool,
        /// Sync performance metrics only
        #[arg(long)]
        performance: bool,
        /// Start date (YYYY-MM-DD)
        #[arg(long)]
        from: Option<String>,
        /// End date (YYYY-MM-DD)
        #[arg(long)]
        to: Option<String>,
        /// Dry run (plan only, don't execute)
        #[arg(long)]
        dry_run: bool,
        /// Use simple text output instead of fancy TUI
        #[arg(long)]
        simple: bool,
    },
    /// Show sync status
    Status {
        /// Database file path
        #[arg(long)]
        db: Option<String>,
    },
    /// Reset failed tasks to pending
    Reset {
        /// Database file path
        #[arg(long)]
        db: Option<String>,
    },
    /// Clear all pending tasks
    Clear {
        /// Database file path
        #[arg(long)]
        db: Option<String>,
    },
}

#[tokio::main]
async fn main() -> garmin_cli::Result<()> {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Auth { command } => match command {
            AuthCommands::Login { email } => commands::login(email, cli.profile).await,
            AuthCommands::Logout => commands::logout(cli.profile).await,
            AuthCommands::Status => commands::status(cli.profile).await,
        },
        Commands::Activities { command } => match command {
            ActivityCommands::List { limit, start } => {
                commands::list_activities(limit, start, cli.profile).await
            }
            ActivityCommands::Get { id } => commands::get_activity(id, cli.profile).await,
            ActivityCommands::Download {
                id,
                file_type,
                output,
            } => commands::download_activity(id, &file_type, output, cli.profile).await,
            ActivityCommands::Upload { file } => {
                commands::upload_activity(&file, cli.profile).await
            }
        },
        Commands::Health { command } => match command {
            HealthCommands::Summary { date } => commands::summary(date, cli.profile).await,
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
            HealthCommands::BodyBattery { date, days } => {
                if let Some(d) = days {
                    commands::body_battery_range(d, cli.profile).await
                } else {
                    commands::body_battery(date, cli.profile).await
                }
            }
            HealthCommands::HeartRate { date } => commands::heart_rate(date, cli.profile).await,
            HealthCommands::Steps { days } => commands::steps(Some(days), cli.profile).await,
            HealthCommands::Calories { days } => commands::calories(Some(days), cli.profile).await,
            HealthCommands::Vo2max { date } => commands::vo2max(date, cli.profile).await,
            HealthCommands::TrainingReadiness { date, days } => {
                if let Some(d) = days {
                    commands::training_readiness_range(d, cli.profile).await
                } else {
                    commands::training_readiness(date, cli.profile).await
                }
            }
            HealthCommands::TrainingStatus { date, days } => {
                if let Some(d) = days {
                    commands::training_status_range(d, cli.profile).await
                } else {
                    commands::training_status(date, cli.profile).await
                }
            }
            HealthCommands::Hrv { date } => commands::hrv(date, cli.profile).await,
            HealthCommands::FitnessAge { date } => commands::fitness_age(date, cli.profile).await,
            HealthCommands::Weight { from, to } => {
                commands::list_weight(from, to, cli.profile).await
            }
            HealthCommands::WeightAdd { weight, unit } => {
                commands::add_weight(weight, &unit, cli.profile).await
            }
            HealthCommands::LactateThreshold { days } => {
                commands::lactate_threshold(Some(days), cli.profile).await
            }
            HealthCommands::RacePredictions { date } => {
                commands::race_predictions(date, cli.profile).await
            }
            HealthCommands::EnduranceScore { days } => {
                commands::endurance_score(Some(days), cli.profile).await
            }
            HealthCommands::HillScore { days } => {
                commands::hill_score(Some(days), cli.profile).await
            }
            HealthCommands::Spo2 { date } => commands::spo2(date, cli.profile).await,
            HealthCommands::Respiration { date } => commands::respiration(date, cli.profile).await,
            HealthCommands::IntensityMinutes { date } => {
                commands::intensity_minutes(date, cli.profile).await
            }
            HealthCommands::BloodPressure { from, to } => {
                commands::blood_pressure(from, to, cli.profile).await
            }
            HealthCommands::Hydration { date } => commands::hydration(date, cli.profile).await,
            HealthCommands::PersonalRecords => commands::personal_records(cli.profile).await,
            HealthCommands::PerformanceSummary { date } => {
                commands::performance_summary(date, cli.profile).await
            }
            HealthCommands::Insights { days } => commands::insights(days, cli.profile).await,
        },
        Commands::Devices { command } => match command {
            DeviceCommands::List => commands::list_devices(cli.profile).await,
            DeviceCommands::Get { id } => commands::get_device(&id, cli.profile).await,
            DeviceCommands::History { db } => commands::device_history(db).await,
        },
        Commands::Profile { command } => match command {
            ProfileCommands::Show => commands::show_profile(cli.profile).await,
            ProfileCommands::Settings => commands::show_settings(cli.profile).await,
        },
        Commands::Sync { command } => match command {
            SyncCommands::Run {
                db,
                activities,
                health,
                performance,
                from,
                to,
                dry_run,
                simple,
            } => {
                commands::sync_run(
                    cli.profile,
                    db,
                    activities,
                    health,
                    performance,
                    from,
                    to,
                    dry_run,
                    simple,
                )
                .await
            }
            SyncCommands::Status { db } => commands::sync_status(cli.profile, db).await,
            SyncCommands::Reset { db } => commands::sync_reset(db).await,
            SyncCommands::Clear { db } => commands::sync_clear(db).await,
        },
    };

    if let Err(e) = result {
        eprintln!("Error: {}", garmin_cli::error::format_user_error(&e));
        std::process::exit(1);
    }

    Ok(())
}
