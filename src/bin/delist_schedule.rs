use anyhow::Result;
use chrono::{Duration as ChronoDuration, SecondsFormat};
use clap::Parser;
use log::warn;
use mkt_signal::common::delist_schedule::{
    default_monitored_venues, provider_for_venue, DelistEvent, DelistScheduleQuery,
};
use mkt_signal::signal::common::TradingVenue;

#[derive(Parser)]
#[command(name = "delist_schedule")]
#[command(about = "Query future symbol delist schedules by venue")]
struct Args {
    /// Venue to query. If omitted, queries the default monitored venues.
    #[arg(short, long)]
    venue: Option<TradingVenue>,

    /// Look-ahead window in days.
    #[arg(long, default_value_t = 7)]
    days: i64,

    /// Exit non-zero if a venue query fails.
    #[arg(long)]
    strict: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    let venues = args
        .venue
        .map(|venue| vec![venue])
        .unwrap_or_else(default_monitored_venues);
    let query = DelistScheduleQuery {
        now: chrono::Utc::now(),
        window: ChronoDuration::days(args.days),
    };

    let mut all_events = Vec::new();
    let mut failures = Vec::new();
    for venue in venues {
        let provider = provider_for_venue(venue);
        match provider.future_delist_events(&query).await {
            Ok(mut events) => all_events.append(&mut events),
            Err(err) => {
                let msg = format!("{}: {err:#}", venue.data_pub_slug());
                if args.strict {
                    anyhow::bail!(msg);
                }
                warn!("delist schedule query failed: {msg}");
                failures.push(msg);
            }
        }
    }

    all_events.sort_by(|left, right| {
        left.delist_time
            .cmp(&right.delist_time)
            .then_with(|| left.risk_type.cmp(right.risk_type))
            .then_with(|| left.venue.data_pub_slug().cmp(right.venue.data_pub_slug()))
            .then_with(|| left.symbol.cmp(&right.symbol))
    });

    print_events(&all_events);
    if !failures.is_empty() {
        println!("failed_venues\t{}", failures.join("; "));
    }
    Ok(())
}

fn print_events(events: &[DelistEvent]) {
    println!(
        "venue\tmarket\tsymbol\tdelist_time_utc\tdelist_time_beijing\trisk_type\tstatus\tsource\tdetail"
    );
    for event in events {
        let (utc, beijing) = match event.delist_time {
            Some(delist_time) => (
                delist_time.to_rfc3339_opts(SecondsFormat::Secs, true),
                (delist_time + ChronoDuration::hours(8))
                    .format("%Y-%m-%d %H:%M:%S+08:00")
                    .to_string(),
            ),
            None => ("NA".to_string(), "NA".to_string()),
        };
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            event.venue.data_pub_slug(),
            event.market,
            event.symbol,
            utc,
            beijing,
            event.risk_type,
            event.status.as_deref().unwrap_or(""),
            event.source,
            event.detail.as_deref().unwrap_or("")
        );
    }
}
