use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::{net::TcpListener, sync::RwLock};

use crate::{errors::MQError, mq::MQ};
use crate::{mq::ArcMQ, topic::Topic as QueueTopic};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topic {
    pub name: String,
    pub msg_count: u64,
    pub msg_size: u64,
    pub channels: HashMap<String, Channel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Channel {
    pub name: String,
    pub clients: HashMap<u64, Client>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Client {
    pub id: u64,
}

#[derive(Debug, Deserialize)]
struct CreateTopicRequest {
    topic: String,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

impl Topic {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            msg_count: 0,
            msg_size: 0,
            channels: HashMap::new(),
        }
    }
}

pub async fn start_http(mq: ArcMQ) -> Result<(), MQError> {
    let app = Router::new()
        // POST /api/v1/topics
        // GET  /api/v1/topics
        .route("/api/v1/topics", post(create_topic).get(list_topics))
        // GET /api/v1/topics/:topic
        .route("/api/v1/topics/{topic_name}", get(get_topic))
        .with_state(mq);

    let addr: SocketAddr = "0.0.0.0:6060".parse().unwrap();
    println!("listening on http://{}", addr);

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn create_topic(
    State(mq): State<ArcMQ>,
    Json(payload): Json<CreateTopicRequest>,
) -> impl IntoResponse {
    let topic_name = payload.topic.trim();
    if topic_name.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorBody {
                error: "topic cannot be empty".into(),
            }),
        )
            .into_response();
    }

    let mut mq = mq.write().await;
    let t = QueueTopic::new(topic_name, 1000);

    match mq.create_topic(t) {
        Ok(_) => {
            return (StatusCode::CREATED, Json(Topic::new(&topic_name))).into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorBody {
                    error: e.to_string(),
                }),
            )
                .into_response();
        }
    }
}

async fn get_topic(
    Path(topic_name): Path<String>,
    State(mq): State<Arc<RwLock<MQ>>>,
) -> impl IntoResponse {
    match get_mq_topic(mq, &topic_name).await {
        Some(t_resp) => {
            println!("get topic: {:?}", t_resp);
            (StatusCode::OK, Json(t_resp.clone())).into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("topic '{}' not found", topic_name),
            }),
        )
            .into_response(),
    }
}

async fn list_topics(State(mq): State<Arc<RwLock<MQ>>>) -> impl IntoResponse {
    let mut topics: Vec<Topic> = get_mq_topics(mq).await;
    topics.sort_by(|a, b| a.name.cmp(&b.name));
    (StatusCode::OK, Json(topics))
}

async fn get_mq_topics(mq: Arc<RwLock<MQ>>) -> Vec<Topic> {
    let mq_clone = mq.clone();

    let mq_lock = mq.read().await;
    let topics = mq_lock.get_topics();
    let mut simple_topics = vec![];
    for t in topics {
        match get_mq_topic(mq_clone.clone(), &t.name).await {
            Some(tt) => simple_topics.push(tt),
            None => {}
        }
    }

    simple_topics
}

async fn get_mq_topic(mq: Arc<RwLock<MQ>>, topic_name: &str) -> Option<Topic> {
    let mq = mq.read().await;

    match mq.get_topic(&topic_name) {
        Some(t) => {
            let chans = mq.get_channels(&topic_name).await;
            let mut simple_chans = HashMap::new();

            for ch in chans {
                let clients = mq.get_clients(&ch.name);
                let mut client_hash = HashMap::new();
                for cli in clients {
                    client_hash.insert(cli.id, Client { id: cli.id });
                }

                simple_chans.insert(
                    ch.name.clone(),
                    Channel {
                        name: ch.name.clone(),
                        clients: client_hash,
                    },
                );
            }

            let t_resp = Topic {
                name: t.name.clone(),
                msg_count: t.msg_count,
                msg_size: t.msg_size,
                channels: simple_chans,
            };

            Some(t_resp)
        }
        None => None,
    }
}
