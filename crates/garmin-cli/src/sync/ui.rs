//! Fancy terminal UI using Ratatui for sync progress visualization

use std::io::{self, stdout, Stdout};
use std::time::Duration;

use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    prelude::CrosstermBackend,
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, Paragraph, Sparkline},
    Frame, Terminal,
};

use super::progress::{SharedProgress, StreamProgress};

/// Color theme for the TUI
pub struct Theme {
    pub activities: Color,
    pub gpx: Color,
    pub health: Color,
    pub performance: Color,
    pub error: Color,
    pub success: Color,
    pub border: Color,
    pub title: Color,
    pub dim: Color,
}

impl Default for Theme {
    fn default() -> Self {
        Self {
            activities: Color::Cyan,
            gpx: Color::Blue,
            health: Color::Green,
            performance: Color::Magenta,
            error: Color::Red,
            success: Color::Green,
            border: Color::DarkGray,
            title: Color::White,
            dim: Color::Gray,
        }
    }
}

/// Terminal UI for sync progress
pub struct SyncUI {
    progress: SharedProgress,
    theme: Theme,
}

impl SyncUI {
    /// Create a new sync UI
    pub fn new(progress: SharedProgress) -> Self {
        Self {
            progress,
            theme: Theme::default(),
        }
    }

    /// Draw the UI to the terminal frame
    pub fn draw(&self, frame: &mut Frame) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .margin(1)
            .constraints([
                Constraint::Length(3),  // Header
                Constraint::Length(12), // Progress bars (3 lines each x 4)
                Constraint::Length(4),  // Stats
                Constraint::Length(3),  // Latest task
            ])
            .split(frame.area());

        self.draw_header(frame, chunks[0]);
        self.draw_progress_bars(frame, chunks[1]);
        self.draw_stats(frame, chunks[2]);
        self.draw_latest(frame, chunks[3]);
    }

    /// Draw the header section
    fn draw_header(&self, frame: &mut Frame, area: Rect) {
        let profile = self.progress.get_profile();
        let date_range = self.progress.get_date_range();

        let header_text = vec![Line::from(vec![
            Span::styled("  Profile: ", Style::default().fg(self.theme.dim)),
            Span::styled(&profile, Style::default().fg(self.theme.title).bold()),
            Span::raw("    "),
            Span::styled(&date_range, Style::default().fg(self.theme.dim)),
        ])];

        let header = Paragraph::new(header_text).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(self.theme.border))
                .title(Span::styled(
                    " Garmin Sync ",
                    Style::default()
                        .fg(self.theme.title)
                        .add_modifier(Modifier::BOLD),
                )),
        );

        frame.render_widget(header, area);
    }

    /// Draw progress bars for each stream
    fn draw_progress_bars(&self, frame: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Length(3),
                Constraint::Length(3),
                Constraint::Length(3),
            ])
            .split(area);

        self.draw_gauge(
            frame,
            chunks[0],
            &self.progress.activities,
            self.theme.activities,
            "Activities",
        );
        self.draw_gauge(
            frame,
            chunks[1],
            &self.progress.gpx,
            self.theme.gpx,
            "GPX Downloads",
        );
        self.draw_gauge(
            frame,
            chunks[2],
            &self.progress.health,
            self.theme.health,
            "Health",
        );
        self.draw_gauge(
            frame,
            chunks[3],
            &self.progress.performance,
            self.theme.performance,
            "Performance",
        );
    }

    /// Draw a single progress gauge with visual bar
    fn draw_gauge(
        &self,
        frame: &mut Frame,
        area: Rect,
        stream: &StreamProgress,
        color: Color,
        title: &str,
    ) {
        let total = stream.get_total();
        let completed = stream.get_completed();
        let failed = stream.get_failed();
        let percent = stream.percent();

        let label = if total == 0 {
            "waiting...".to_string()
        } else if failed > 0 {
            format!("{}/{} ({} failed) {}%", completed, total, failed, percent)
        } else {
            format!("{}/{} {}%", completed, total, percent)
        };

        let gauge = Gauge::default()
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(self.theme.border))
                    .title(Span::styled(
                        format!(" {} ", title),
                        Style::default().fg(color).add_modifier(Modifier::BOLD),
                    )),
            )
            .gauge_style(Style::default().fg(color).bg(Color::DarkGray))
            .percent(percent)
            .label(Span::styled(
                label,
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ));

        frame.render_widget(gauge, area);
    }

    /// Draw statistics section with sparkline
    fn draw_stats(&self, frame: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(area);

        // Rate sparkline
        let rate_history = self.progress.rate_history.lock().unwrap();
        let data: Vec<u64> = rate_history.iter().map(|&x| x as u64).collect();
        drop(rate_history);

        let sparkline = Sparkline::default()
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(self.theme.border))
                    .title(" Rate "),
            )
            .data(&data)
            .style(Style::default().fg(self.theme.success));

        frame.render_widget(sparkline, chunks[0]);

        // Stats text
        let rpm = self.progress.requests_per_minute();
        let elapsed = self.progress.elapsed_str();
        let eta = self.progress.eta_str();
        let errors = self.progress.total_failed();

        let stats_text = vec![
            Line::from(vec![
                Span::styled("  Rate: ", Style::default().fg(self.theme.dim)),
                Span::styled(
                    format!("{} req/min", rpm),
                    Style::default().fg(self.theme.title),
                ),
                Span::raw("  "),
                Span::styled("Elapsed: ", Style::default().fg(self.theme.dim)),
                Span::styled(&elapsed, Style::default().fg(self.theme.title)),
            ]),
            Line::from(vec![
                Span::styled("  ETA: ", Style::default().fg(self.theme.dim)),
                Span::styled(&eta, Style::default().fg(self.theme.title)),
                Span::raw("  "),
                Span::styled("Errors: ", Style::default().fg(self.theme.dim)),
                Span::styled(
                    errors.to_string(),
                    Style::default().fg(if errors > 0 {
                        self.theme.error
                    } else {
                        self.theme.success
                    }),
                ),
            ]),
        ];

        let stats = Paragraph::new(stats_text).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(self.theme.border))
                .title(" Stats "),
        );

        frame.render_widget(stats, chunks[1]);
    }

    /// Draw the latest task section
    fn draw_latest(&self, frame: &mut Frame, area: Rect) {
        // Find the most recently updated stream
        let latest = self.get_latest_item();

        let text = vec![Line::from(vec![
            Span::styled("  [Latest] ", Style::default().fg(self.theme.dim)),
            Span::styled(&latest, Style::default().fg(self.theme.title)),
        ])];

        let latest_widget = Paragraph::new(text).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(self.theme.border)),
        );

        frame.render_widget(latest_widget, area);
    }

    /// Get the most recently updated item description
    fn get_latest_item(&self) -> String {
        // Check each stream for the latest item
        let streams = [
            &self.progress.activities,
            &self.progress.gpx,
            &self.progress.health,
            &self.progress.performance,
        ];

        for stream in streams.iter().rev() {
            let item = stream.get_last_item();
            if !item.is_empty() {
                return format!("{}: {}", stream.name, item);
            }
        }

        "Waiting for tasks...".to_string()
    }
}

/// Setup terminal for TUI mode
pub fn setup_terminal() -> io::Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    Terminal::new(backend)
}

/// Restore terminal to normal mode
pub fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> io::Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

/// Run the TUI event loop (call from tokio::spawn)
pub async fn run_tui(progress: SharedProgress) -> io::Result<()> {
    let mut terminal = setup_terminal()?;
    let ui = SyncUI::new(progress.clone());

    loop {
        terminal.draw(|f| ui.draw(f))?;

        // Poll for events with timeout
        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => {
                            break;
                        }
                        // Handle Ctrl+C
                        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        // Update rate history every second
        progress.update_rate_history();

        // Check if sync is complete
        if progress.is_complete() {
            // Give user a moment to see final state
            tokio::time::sleep(Duration::from_secs(1)).await;
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    restore_terminal(&mut terminal)?;
    Ok(())
}

/// Handle TUI cleanup on panic or signal
pub struct TuiCleanupGuard {
    pub terminal: Option<Terminal<CrosstermBackend<Stdout>>>,
}

impl Drop for TuiCleanupGuard {
    fn drop(&mut self) {
        if let Some(mut terminal) = self.terminal.take() {
            let _ = restore_terminal(&mut terminal);
        }
    }
}
