//! Terminal UI using Ratatui for sync progress visualization
//!
//! Inspired by Toad's clean, minimal design.

use std::io::{self, stdout, Stdout};
use std::time::{Duration, Instant};

use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    prelude::CrosstermBackend,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Frame, Terminal,
};

use super::progress::{PlanningStep, SharedProgress, StreamProgress};

/// Color theme matching Toad's dark theme
struct Theme {
    // Task states
    completed: Color,
    in_progress: Color,
    pending: Color,
    failed: Color,

    // UI elements
    title: Color,
    dim: Color,
    border: Color,
    bar_filled: Color,
    bar_empty: Color,
}

impl Default for Theme {
    fn default() -> Self {
        Self {
            completed: Color::Green,
            in_progress: Color::Yellow,
            pending: Color::DarkGray,
            failed: Color::Red,

            title: Color::White,
            dim: Color::Gray,
            border: Color::DarkGray,
            bar_filled: Color::Cyan,
            bar_empty: Color::DarkGray,
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
        // Check if we're in planning phase
        if self.progress.is_planning() {
            self.draw_planning_screen(frame);
            return;
        }

        let errors = self.progress.get_errors();
        let has_errors = !errors.is_empty();

        // Calculate layout based on whether we have errors
        let constraints = if has_errors {
            vec![
                Constraint::Length(5),  // Header
                Constraint::Length(6),  // Tasks
                Constraint::Length(4 + errors.len().min(5) as u16), // Errors (max 5 shown)
                Constraint::Length(3),  // Current task
                Constraint::Length(3),  // Stats
                Constraint::Length(1),  // Footer
            ]
        } else {
            vec![
                Constraint::Length(5),  // Header
                Constraint::Length(6),  // Tasks
                Constraint::Length(3),  // Current task
                Constraint::Length(3),  // Stats
                Constraint::Length(1),  // Footer
            ]
        };

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .margin(1)
            .constraints(constraints)
            .split(frame.area());

        let mut idx = 0;

        self.draw_header(frame, chunks[idx]);
        idx += 1;

        self.draw_tasks(frame, chunks[idx]);
        idx += 1;

        if has_errors {
            self.draw_errors(frame, chunks[idx], &errors);
            idx += 1;
        }

        self.draw_current_task(frame, chunks[idx]);
        idx += 1;

        self.draw_stats(frame, chunks[idx]);
        idx += 1;

        self.draw_footer(frame, chunks[idx]);
    }

    /// Draw the planning screen (centered)
    fn draw_planning_screen(&self, frame: &mut Frame) {
        let area = frame.area();
        let step = self.progress.get_planning_step();
        let profile = self.progress.get_profile();
        let storage_path = self.progress.get_storage_path();
        let oldest_date = self.progress.get_oldest_activity_date();

        // Build the content lines
        let mut lines = vec![
            Line::from(""),
            Line::from(Span::styled(
                "Garmin Sync",
                Style::default()
                    .fg(self.theme.title)
                    .add_modifier(Modifier::BOLD),
            )),
            Line::from(""),
        ];

        // Show profile if we have it
        if !profile.is_empty() {
            lines.push(Line::from(vec![
                Span::styled("Profile: ", Style::default().fg(self.theme.dim)),
                Span::styled(&profile, Style::default().fg(self.theme.title)),
            ]));
            lines.push(Line::from(""));
        }

        // Planning steps with checkmarks
        let steps = [
            (PlanningStep::FetchingProfile, "Fetch profile"),
            (PlanningStep::FindingOldestActivity, "Find oldest activity"),
            (PlanningStep::PlanningActivities, "Plan activity sync"),
            (PlanningStep::PlanningHealth { days: 0 }, "Plan health sync"),
            (PlanningStep::PlanningPerformance { weeks: 0 }, "Plan performance sync"),
        ];

        for (check_step, label) in steps {
            let (icon, style) = if step == PlanningStep::Complete {
                ("✓", Style::default().fg(self.theme.completed))
            } else if std::mem::discriminant(&step) == std::mem::discriminant(&check_step) {
                ("➔", Style::default().fg(self.theme.in_progress))
            } else if self.step_is_before(&step, &check_step) {
                ("○", Style::default().fg(self.theme.pending))
            } else {
                ("✓", Style::default().fg(self.theme.completed))
            };

            let mut line_spans = vec![
                Span::styled(format!("  {} ", icon), style),
                Span::styled(label, style),
            ];

            // Add extra info for completed steps
            if icon == "✓" {
                match &check_step {
                    PlanningStep::FindingOldestActivity => {
                        if let Some(ref date) = oldest_date {
                            line_spans.push(Span::styled(
                                format!(" → {}", date),
                                Style::default().fg(self.theme.dim),
                            ));
                        }
                    }
                    PlanningStep::PlanningHealth { .. } => {
                        if let PlanningStep::PlanningHealth { days } = &step {
                            if *days > 0 {
                                line_spans.push(Span::styled(
                                    format!(" ({} days)", days),
                                    Style::default().fg(self.theme.dim),
                                ));
                            }
                        }
                    }
                    PlanningStep::PlanningPerformance { .. } => {
                        if let PlanningStep::PlanningPerformance { weeks } = &step {
                            if *weeks > 0 {
                                line_spans.push(Span::styled(
                                    format!(" ({} weeks)", weeks),
                                    Style::default().fg(self.theme.dim),
                                ));
                            }
                        }
                    }
                    _ => {}
                }
            }

            // Show current step details
            if std::mem::discriminant(&step) == std::mem::discriminant(&check_step) {
                match &step {
                    PlanningStep::PlanningHealth { days } if *days > 0 => {
                        line_spans.push(Span::styled(
                            format!(" ({} days)", days),
                            Style::default().fg(self.theme.in_progress),
                        ));
                    }
                    PlanningStep::PlanningPerformance { weeks } if *weeks > 0 => {
                        line_spans.push(Span::styled(
                            format!(" ({} weeks)", weeks),
                            Style::default().fg(self.theme.in_progress),
                        ));
                    }
                    _ => {}
                }
            }

            lines.push(Line::from(line_spans));
        }

        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            &storage_path,
            Style::default().fg(self.theme.dim),
        )));

        // Center the content
        let content_height = lines.len() as u16;
        let content_width = 50u16; // Approximate width

        let vertical_padding = area.height.saturating_sub(content_height) / 2;
        let horizontal_padding = area.width.saturating_sub(content_width) / 2;

        let centered_area = Rect {
            x: area.x + horizontal_padding,
            y: area.y + vertical_padding,
            width: content_width.min(area.width),
            height: content_height.min(area.height),
        };

        let paragraph = Paragraph::new(lines)
            .alignment(ratatui::layout::Alignment::Center);

        frame.render_widget(paragraph, centered_area);

        // Footer at bottom
        let footer_area = Rect {
            x: area.x + 1,
            y: area.y + area.height - 2,
            width: area.width - 2,
            height: 1,
        };

        let footer = Paragraph::new(Line::from(vec![
            Span::styled("q ", Style::default().fg(self.theme.dim)),
            Span::styled("quit", Style::default().fg(self.theme.dim)),
        ]));

        frame.render_widget(footer, footer_area);
    }

    /// Check if current step is before the check step
    fn step_is_before(&self, current: &PlanningStep, check: &PlanningStep) -> bool {
        let order = |s: &PlanningStep| match s {
            PlanningStep::FetchingProfile => 0,
            PlanningStep::FindingOldestActivity => 1,
            PlanningStep::PlanningActivities => 2,
            PlanningStep::PlanningHealth { .. } => 3,
            PlanningStep::PlanningPerformance { .. } => 4,
            PlanningStep::Complete => 5,
        };
        order(current) < order(check)
    }

    /// Draw the header section
    fn draw_header(&self, frame: &mut Frame, area: Rect) {
        let profile = self.progress.get_profile();
        let date_range = self.progress.get_date_range();

        let lines = vec![
            Line::from(""),
            Line::from(vec![
                Span::styled(
                    "  Garmin Sync",
                    Style::default()
                        .fg(self.theme.title)
                        .add_modifier(Modifier::BOLD),
                ),
            ]),
            Line::from(vec![
                Span::styled("  Profile: ", Style::default().fg(self.theme.dim)),
                Span::styled(&profile, Style::default().fg(self.theme.title)),
                Span::raw("    "),
                Span::styled(&date_range, Style::default().fg(self.theme.dim)),
            ]),
        ];

        let header = Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(Style::default().fg(self.theme.border)),
        );

        frame.render_widget(header, area);
    }

    /// Draw the tasks section with checklist style
    fn draw_tasks(&self, frame: &mut Frame, area: Rect) {
        let lines = vec![
            Line::from(""),
            self.task_line(&self.progress.activities),
            self.task_line(&self.progress.gpx),
            self.task_line(&self.progress.health),
            self.task_line(&self.progress.performance),
        ];

        let tasks = Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(Style::default().fg(self.theme.border))
                .title(Span::styled(
                    " Tasks ",
                    Style::default().fg(self.theme.dim),
                )),
        );

        frame.render_widget(tasks, area);
    }

    /// Create a task line with status icon and progress
    fn task_line(&self, stream: &StreamProgress) -> Line<'static> {
        let total = stream.get_total();
        let completed = stream.get_completed();
        let failed = stream.get_failed();
        let percent = stream.percent();

        // Determine state
        let (icon, icon_color, name_style) = if total == 0 {
            // Pending (not started)
            ("○", self.theme.pending, Style::default().fg(self.theme.pending))
        } else if stream.is_complete() {
            if failed > 0 {
                // Completed with errors
                ("✓", self.theme.completed, Style::default().fg(self.theme.dim).add_modifier(Modifier::CROSSED_OUT))
            } else {
                // Completed successfully
                ("✓", self.theme.completed, Style::default().fg(self.theme.dim).add_modifier(Modifier::CROSSED_OUT))
            }
        } else {
            // In progress
            ("➔", self.theme.in_progress, Style::default().fg(self.theme.title))
        };

        let mut spans = vec![
            Span::raw("  "),
            Span::styled(icon, Style::default().fg(icon_color)),
            Span::raw(" "),
            Span::styled(format!("{:<15}", stream.name), name_style),
        ];

        if total > 0 {
            // Add count
            let count_str = format!("{:>4}/{:<4}", completed, total);
            spans.push(Span::styled(count_str, Style::default().fg(self.theme.dim)));

            // Add failed count if any
            if failed > 0 {
                spans.push(Span::styled(
                    format!("  ({} failed)", failed),
                    Style::default().fg(self.theme.failed),
                ));
            }

            // Add progress bar if in progress
            if !stream.is_complete() && total > 0 {
                spans.push(Span::raw("  "));
                spans.extend(self.progress_bar(percent, 20));
                spans.push(Span::styled(
                    format!(" {}%", percent),
                    Style::default().fg(self.theme.dim),
                ));
            }
        }

        Line::from(spans)
    }

    /// Create a progress bar as spans
    fn progress_bar(&self, percent: u16, width: u16) -> Vec<Span<'static>> {
        let filled = (percent as u32 * width as u32 / 100) as u16;
        let empty = width - filled;

        vec![
            Span::styled(
                "█".repeat(filled as usize),
                Style::default().fg(self.theme.bar_filled),
            ),
            Span::styled(
                "░".repeat(empty as usize),
                Style::default().fg(self.theme.bar_empty),
            ),
        ]
    }

    /// Draw the errors section
    fn draw_errors(&self, frame: &mut Frame, area: Rect, errors: &[super::progress::ErrorEntry]) {
        let mut lines = vec![Line::from("")];

        for error in errors.iter().take(5) {
            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled("✗", Style::default().fg(self.theme.failed)),
                Span::raw(" "),
                Span::styled(error.stream.to_string(), Style::default().fg(self.theme.failed)),
                Span::raw(" "),
                Span::styled(error.item.clone(), Style::default().fg(self.theme.dim)),
                Span::raw(": "),
                Span::styled(error.error.clone(), Style::default().fg(self.theme.dim)),
            ]));
        }

        if errors.len() > 5 {
            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled(
                    format!("  ... and {} more", errors.len() - 5),
                    Style::default().fg(self.theme.dim),
                ),
            ]));
        }

        let errors_widget = Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(Style::default().fg(self.theme.border))
                .title(Span::styled(
                    format!(" Errors ({}) ", errors.len()),
                    Style::default().fg(self.theme.failed),
                )),
        );

        frame.render_widget(errors_widget, area);
    }

    /// Draw the current task section
    fn draw_current_task(&self, frame: &mut Frame, area: Rect) {
        let current = self.progress.get_current_task();
        let task_text = current.unwrap_or_else(|| "Waiting...".to_string());

        let lines = vec![Line::from(vec![
            Span::raw("  "),
            Span::styled("➔ ", Style::default().fg(self.theme.in_progress)),
            Span::styled(task_text, Style::default().fg(self.theme.title)),
        ])];

        let current_widget = Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(Style::default().fg(self.theme.border)),
        );

        frame.render_widget(current_widget, area);
    }

    /// Draw the stats section
    fn draw_stats(&self, frame: &mut Frame, area: Rect) {
        let rpm = self.progress.requests_per_minute();
        let elapsed = self.progress.elapsed_str();
        let eta = self.progress.eta_str();

        let lines = vec![Line::from(vec![
            Span::raw("  "),
            Span::styled("⚡", Style::default().fg(self.theme.in_progress)),
            Span::raw(" "),
            Span::styled(format!("{} req/min", rpm), Style::default().fg(self.theme.title)),
            Span::raw("    "),
            Span::styled("⏱", Style::default().fg(self.theme.dim)),
            Span::raw(" "),
            Span::styled(&elapsed, Style::default().fg(self.theme.title)),
            Span::raw("    "),
            Span::styled("◔", Style::default().fg(self.theme.dim)),
            Span::raw(" "),
            Span::styled(&eta, Style::default().fg(self.theme.title)),
        ])];

        let stats = Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(Style::default().fg(self.theme.border)),
        );

        frame.render_widget(stats, area);
    }

    /// Draw the footer with shortcuts and path
    fn draw_footer(&self, frame: &mut Frame, area: Rect) {
        let storage_path = self.progress.get_storage_path();

        let footer = Paragraph::new(Line::from(vec![
            Span::styled("  q ", Style::default().fg(self.theme.dim)),
            Span::styled("quit", Style::default().fg(self.theme.dim)),
            Span::raw("                                        "),
            Span::styled(&storage_path, Style::default().fg(self.theme.dim)),
        ]));

        frame.render_widget(footer, area);
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
    let mut guard = TuiCleanupGuard {
        terminal: Some(setup_terminal()?),
    };
    let ui = SyncUI::new(progress.clone());
    let mut last_rate_tick = Instant::now();

    loop {
        if let Some(terminal) = guard.terminal.as_mut() {
            terminal.draw(|f| ui.draw(f))?;
        }

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
        if last_rate_tick.elapsed() >= Duration::from_secs(1) {
            progress.update_rate_history();
            last_rate_tick = Instant::now();
        }

        // Check if sync is complete (but not during planning)
        if progress.should_shutdown() || (!progress.is_planning() && progress.is_complete()) {
            // Give user a moment to see final state
            tokio::time::sleep(Duration::from_secs(1)).await;
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    if let Some(mut terminal) = guard.terminal.take() {
        restore_terminal(&mut terminal)?;
    }
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
