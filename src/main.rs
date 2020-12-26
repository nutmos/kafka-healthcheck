use actix_web::{get, App, HttpServer, HttpResponse};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use std::vec::Vec;
use clap::{App as ClapApp, Arg, ArgMatches};
use lazy_static::lazy_static;
use std::sync::Mutex;
use log::{info, error, LevelFilter};
use simple_logger::SimpleLogger;
use std::panic;

lazy_static! {
    static ref KAFKA_ARG_MATCHES: Mutex<Vec<ArgMatches<'static>>> = Mutex::new(vec![]);
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
enum KafkaStatus {
    Green,
    Yellow,
    Red,
}
#[derive(Debug, Serialize, Deserialize)]
struct KafkaPartitionDetail {
    topic: String,
    partition: i32,
    replicas: Vec<i32>,
    isr: Vec<i32>,
}
#[derive(Debug, Serialize, Deserialize)]
struct KafkaHealth {
    status: KafkaStatus,
    brokers: usize,
    topics: usize,
    out_of_sync_partitions: Option<Vec<KafkaPartitionDetail>>,
}

macro_rules! unwrap_argmatches {
    ($matches: ident, $value_of: tt) => {
        match $matches.value_of($value_of) {
            Some(v) => {
                info!("{}: {}", $value_of, v);
                v
            },
            None => {
                error!("No input value for {}.", $value_of);
                panic!();
            }
        }
    };
}

#[get("/health")]
async fn health() -> HttpResponse {
    let arg_matches = &KAFKA_ARG_MATCHES.lock().unwrap()[0];
    let bootstrap_servers = unwrap_argmatches!(arg_matches, "bootstrap.servers");
    let security_protocol = unwrap_argmatches!(arg_matches, "security.protocol");
    //let consumer: BaseConsumer = ClientConfig::new()
    //    //.set("bootstrap.servers", "localhost:9092")
    //    .set("bootstrap.servers", arg_matches.value_of("bootstrap.servers").expect("No kafka broker options"))
    //    .set("security.protocol", arg_matches.value_of("security.protocol").expect("No security protocol options"))
    //    .create()
    //    .expect("Consumer creation failed");
    let consumer: BaseConsumer = match (arg_matches.value_of("sasl.username"), arg_matches.value_of("sasl.password")) {
        (None, None) => ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("security.protocol", security_protocol)
            .create()
            .expect("Consumer creation failed"),
        (Some(u), Some(p)) => ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("security.protocol", security_protocol)
            .set("sasl.username", u)
            .set("sasl.password", p)
            .create()
            .expect("Consumer creation failed"),
        _ => {
            error!("Username should be added with password");
            panic!();
        }
    };
    let metadata = match consumer.fetch_metadata(None, Duration::from_secs(30)) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to fetch metadata. Reason: {}", e);
            panic!();
        }
    };
    let mut health = KafkaHealth {
        status: KafkaStatus::Green,
        brokers: metadata.brokers().len(),
        topics: metadata.topics().len(),
        out_of_sync_partitions: None
    };
    let mut outofsync = Vec::new();
    for topic in metadata.topics() {
        for partition in topic.partitions() {
            if partition.isr().len() == 0 {
                health.status = KafkaStatus::Red;
            }
            if partition.replicas().len() > partition.isr().len() {
                outofsync.push(KafkaPartitionDetail {
                    topic: topic.name().to_string(),
                    partition: partition.id(),
                    replicas: partition.replicas().to_vec(),
                    isr: partition.isr().to_vec()
                });
            }
        }
    }
    if outofsync.len() > 0 && health.status != KafkaStatus::Red {
        health.status = KafkaStatus::Yellow;
    }
    health.out_of_sync_partitions = Some(outofsync);
    HttpResponse::Ok().json(health)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    SimpleLogger::new().with_level(LevelFilter::Info).init().expect("Init simple logger failed");
    let matches: ArgMatches<'static> = ClapApp::new("consumer example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer")
        .arg(
            Arg::with_name("bootstrap.servers")
                .short("b")
                .long("bootstrap.servers")
                .help("Boostrap servers list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("security.protocol")
                .short("s")
                .long("security.protocol")
                .help("Kafka security protocol (plaintext, ssl, sasl_plaintext, sasl_ssl)")
                .takes_value(true)
                .default_value("plaintext"),
        )
        .arg(
            Arg::with_name("sasl.username")
                .short("u")
                .long("sasl.username")
                .help("Username for authenticate with Kafka")
                .takes_value(true)
        )
        .arg(
            Arg::with_name("sasl.password")
                .short("p")
                .long("sasl.password")
                .help("Password for authenticate with Kafka")
                .takes_value(true)
        )
        .get_matches();
    KAFKA_ARG_MATCHES.lock().unwrap().push(matches);
    //kafka_brokers = matches.value_of("brokers");
    //kafka_topics = matches.value_of("topics");
    HttpServer::new(|| App::new().service(health))
        .bind("0.0.0.0:8080")?
        .run()
        .await
}
