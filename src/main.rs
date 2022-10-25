extern crate ini;
use colored::Colorize;
use gdal::vector::{Feature, LayerAccess};
use gdal::Dataset;
use ini::{Ini, Properties};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::Path;
use std::process;
use tokio_postgres::{Client, Error, NoTls};

#[tokio::main]
async fn main() {
    let ini_path = "./config.ini";
    validate_config(ini_path);
    let ini = Ini::load_from_file(ini_path).unwrap();
    let config: &Properties = ini.section(Some("CONFIG")).unwrap();

    let es_auth: HashMap<String, String> = get_elastic_auth(config);

    let chunk_size = 5000;
    let client = match psql_connect(config).await {
        Ok(client) => {
            println!("Connected to PSQL");
            client
        }
        Err(e) => {
            println!("Could not connect to PSQL server: {}", e);
            process::exit(1);
        }
    };
    let dataset = Dataset::open(Path::new("gdb.gdb")).unwrap();
    let nr_of_layers = dataset.layer_count();
    let mut index = 0;

    while index < nr_of_layers {
        let mut layer = dataset.layer(index).unwrap();
        println!(
            "Processing {} layer with {} features",
            layer.name(),
            layer.feature_count()
        );
        let field_def = layer
            .defn()
            .fields()
            .map(|field| (field.name(), field.field_type()))
            .collect::<Vec<_>>();

        let mapping = layer_mapping(field_def);

        let layer_name = layer.name();

        let res = create_elastic_index(
            &mapping,
            config.get("ELASTIC_HOST").unwrap(),
            &layer_name.to_lowercase(),
            &es_auth,
        )
        .await;

        if !res {
            println!(
                "{} {}",
                "Cannot create elastic index: ".red(),
                config.get("ELASTIC_HOST").unwrap()
            );
            process::exit(1);
        }
        let mut chunks: Vec<Vec<Feature>> = Vec::new();
        let mut chunk: Vec<Feature> = Vec::new();

        for feature in layer.features() {
            chunk.push(feature);
            if chunk.len() == chunk_size as usize {
                chunks.push(chunk);
                chunk = Vec::new();
            }
        }
        // if chunk.len() > 0 {
        //     chunks.push(chunk);
        // }

        for chunk in &chunks {
            elastic_insert(
                chunk,
                &mapping,
                config.get("ELASTIC_HOST").unwrap(),
                &layer_name.to_lowercase(),
                &es_auth,
            )
            .await;
        }

        if chunk.len() > 0 {
            chunks.push(chunk);
        }
        index = index + 1;
    }
}

async fn elastic_insert(
    features: &Vec<Feature<'_>>,
    mapping: &HashMap<String, String>,
    es_host: &str,
    index: &str,
    auth: &HashMap<String, String>,
) -> Result<bool, anyhow::Error> {
    let body: Vec<String> = process_rows(features, mapping, index);

    let client = reqwest::Client::new();
    match client
        .put([es_host, "/_bulk?wait_for_active_shards=0"].join(""))
        .body([body.join(""), "\n".to_string()].join(""))
        .basic_auth(
            auth.get("username").unwrap_or(&"".to_string()),
            Some(auth.get("password").unwrap_or(&"".to_string())),
        )
        .header("content-type", "application/json")
        .send()
        .await
    {
        reqwest::Result::Ok(e) => {
            let status = e.status().to_string();
            let text = e.text().await.unwrap();
            if text.contains("FORBIDDEN/12/index read-only") {
                return Err(anyhow::anyhow!(
                    "ElasticSearch Index is READ ONLY, Stopping Import"
                ));
            }
            let json: Value = serde_json::from_str(&text).unwrap_or_else(|e| {
                println!("{} {}", "Error parsing ES response".red(), e);
                process::exit(1);
            });
            if json["errors"].as_bool().unwrap_or(false) {
                let mut errors: i32 = 0;
                for item in json["items"].as_array() {
                    if item.first().unwrap()["index"]["status"] == 400 {
                        errors = errors + 1;
                    }
                }

                if errors > 0 {
                    println!("{} {} {}", "Could not insert ".red(), errors, "rows".red());
                }
            }
            if status == "200 OK" {
                return Ok(true);
            }
            Err(anyhow::anyhow!(
                "ElasticSearch responded with an unknown status: {}",
                status
            ))
        }
        reqwest::Result::Err(e) => Err(anyhow::anyhow!("Request Error {}", e)),
    }
}

fn process_rows(
    features: &Vec<Feature>,
    mapping: &HashMap<String, String>,
    index: &str,
) -> Vec<String> {
    let mut body: Vec<String> = Vec::new();
    for feature in features {
        let mut es_row = json!({});
        body.push(
            [
                json!({"index": {
                    "_index": index,
                    "_type": "data"
                }})
                .to_string(),
                "\n".to_string(),
            ]
            .join(""),
        );
        for e in feature.fields() {
            let mut ignore_value = false;
            let value = match e.1 {
                Some(v) => {
                    let field_type = mapping.get(&e.0).unwrap();
                    if field_type == "string" {
                        match v.into_string() {
                            Some(v) => Value::from(v),
                            None => {
                                ignore_value = true;
                                Value::from("")
                            }
                        }
                    } else if field_type == "integer" {
                        match v.into_int64() {
                            Some(v) => Value::from(v),
                            None => {
                                ignore_value = true;
                                Value::from("")
                            }
                        }
                    } else if field_type == "double" {
                        match v.into_real() {
                            Some(v) => Value::from(v),
                            None => {
                                ignore_value = true;
                                Value::from("")
                            }
                        }
                    } else if field_type == "date" {
                        match v.into_date() {
                            Some(v) => Value::from(v.format("%Y-%m-%d").to_string()),
                            None => {
                                ignore_value = true;
                                Value::from("")
                            }
                        }
                    } else {
                        ignore_value = true;
                        Value::from("")
                    }
                }
                None => {
                    ignore_value = true;
                    Value::from("")
                }
            };

            if !ignore_value {
                es_row[&e.0.to_lowercase()] = value;
            }
        }

        println!("{}", feature.geometry().geometry_type());

        body.push([json!(es_row).to_string(), "\n".to_string()].join(""));
    }
    body
}

async fn psql_connect(config: &Properties) -> Result<Client, Error> {
    let connection_string = [
        "host=",
        config.get("PSQL_HOST").unwrap(),
        " port=",
        config.get("PSQL_PORT").unwrap(),
        " user=",
        config.get("PSQL_USER").unwrap(),
        " password=",
        config.get("PSQL_PASS").unwrap(),
        " dbname=",
        config.get("PSQL_DB").unwrap(),
    ]
    .join("");

    println!("{}", connection_string);

    match tokio_postgres::connect(&connection_string, NoTls).await {
        Ok(res) => Ok(res.0),
        Err(e) => Err(e),
    }
}

async fn create_elastic_index(
    mapping: &HashMap<String, String>,
    es_host: &str,
    index: &str,
    auth: &HashMap<String, String>,
) -> bool {
    let url = [es_host, &index].join("\\");
    let client = reqwest::Client::new();
    match client
        .delete(&url)
        .basic_auth(
            auth.get("username").unwrap(),
            Some(auth.get("password").unwrap()),
        )
        .send()
        .await
    {
        reqwest::Result::Ok(_) => true,
        reqwest::Result::Err(_) => true,
    };

    let data = create_elastic_mapping(&mapping).to_string();

    match client
        .put(&url)
        .body(data)
        .basic_auth(
            auth.get("username").unwrap(),
            Some(auth.get("password").unwrap()),
        )
        .header("content-type", "application/json")
        .send()
        .await
    {
        reqwest::Result::Ok(e) => {
            if e.status().to_string() == "200 OK" {
                return true;
            }
            let text = e.text().await.unwrap();
            println!("{:?}", text);
            false
        }
        reqwest::Result::Err(_) => false,
    }
}

fn create_elastic_mapping(mapping: &HashMap<String, String>) -> Value {
    let mut es_mapping = json!({
        "settings": {
            "index": {
                "refresh_interval": "1s",
                "number_of_shards": 3,
                "number_of_replicas": 0
            },
            "analysis": {
                "analyzer": {
                    "lowercase_analyzer": {
                        "filter": [
                            "lowercase"
                        ],
                        "type": "custom",
                        "tokenizer": "keyword"
                    }
                }
            }
        },
        "mappings":{
            "data":{
                "properties":{

                }
            }
        }
    });

    for key in mapping.keys() {
        let lowercase_key = key.to_lowercase();
        match mapping.get(key).unwrap().as_str() {
            "string" => {
                es_mapping["mappings"]["data"]["properties"][lowercase_key] = json!({
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword"
                      }
                    },
                    "analyzer": "lowercase_analyzer"
                });
            }
            "integer" => {
                es_mapping["mappings"]["data"]["properties"][lowercase_key] = json!({
                  "type": "integer"
                });
            }
            "double" => {
                es_mapping["mappings"]["data"]["properties"][lowercase_key] = json!({
                  "type": "double"
                });
            }
            "date" => {
                es_mapping["mappings"]["data"]["properties"][lowercase_key] = json!({
                  "type": "date"
                });
            }
            &_ => {}
        };
    }

    es_mapping
}

fn layer_mapping(field_def: Vec<(String, u32)>) -> HashMap<String, String> {
    let mut mapping: HashMap<String, String> = HashMap::new();
    for field in field_def.into_iter().enumerate() {
        let f = match field.1 .1 {
            0_u32 => {
                //integer
                mapping.insert(field.1 .0, "integer".to_string());
            }
            2_u32 => {
                //double
                mapping.insert(field.1 .0, "double".to_string());
            }
            4_u32 => {
                //string
                mapping.insert(field.1 .0, "string".to_string());
            }
            11_u32 => {
                //date
                mapping.insert(field.1 .0, "date".to_string());
            }
            _ => {
                println!("not matched {:?}", field.1 .1);
            }
        };
    }

    mapping
}

fn validate_config(ini_path: &str) {
    let ini: Ini = Ini::load_from_file(ini_path).expect("Cannot open ini file:");
    let config: &Properties = ini
        .section(Some("CONFIG"))
        .expect("CONFIG section missing from INI");
    for entry in [
        "PSQL_HOST",
        "PSQL_PORT",
        "PSQL_USER",
        "PSQL_PASS",
        "PSQL_DB",
        "ELASTIC_HOST",
        "ELASTIC_USER",
        "ELASTIC_PASS",
        "ROWS_AT_ONCE",
    ] {
        config.get(entry).or_else(|| {
            println!("{} {}", "Missing Config for".red(), entry);
            process::exit(1);
        });
    }
}

fn get_elastic_auth(config: &Properties) -> HashMap<String, String> {
    let mut es_auth: HashMap<String, String> = HashMap::new();
    let es_username = config.get("ELASTIC_USER").unwrap_or("").to_string();
    let es_password = config.get("ELASTIC_PASS").unwrap_or("").to_string();
    es_auth.insert("username".to_string(), es_username.to_string());
    es_auth.insert("password".to_string(), es_password.to_string());

    es_auth
}
