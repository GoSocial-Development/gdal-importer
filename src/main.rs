extern crate ini;
use gdal::vector::{Feature, FieldValue, LayerAccess};
use gdal::Dataset;
use ini::{Ini, Properties};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::env;
use std::io::{self, Write};
use std::path::Path;
use std::process;
use std::time::Instant;
use tokio_postgres::tls::NoTlsStream;
use tokio_postgres::{Client, Connection, Error, NoTls, Socket};

fn process_resume(string: &str) -> HashMap<String, i64> {
    let mut resume: HashMap<String, i64> = HashMap::new();
    for entry in string.split("|") {
        let split = entry.split(":").collect::<Vec<&str>>();
        if split.len() > 1 {
            resume.insert(split[0].to_string(), split[1].parse::<i64>().unwrap());
        }
    }
    resume
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let gdb_path = &args[1];

    let ini_path = "./config.ini";
    validate_config(ini_path);
    let ini = Ini::load_from_file(ini_path).unwrap();
    let config: &Properties = ini.section(Some("CONFIG")).unwrap();

    let resume = process_resume(config.get("RESUME").unwrap());

    let es_auth: HashMap<String, String> = get_elastic_auth(config);

    let chunk_size = config
        .get("ROWS_AT_ONCE")
        .expect("ROWS_AT_ONCE config missing")
        .to_string()
        .parse::<i32>()
        .expect("ROWS_AT_ONCE config empty");

    let geometry_tolerance = config
        .get("TOLERANCE")
        .unwrap_or("0")
        .to_string()
        .parse::<f64>()
        .expect("Cannot parse TOLERANCE");

    let client = match psql_connect(config).await {
        Ok(result) => {
            tokio::spawn(async move {
                if let Err(e) = result.1.await {
                    println!("PSQL Connection Error: {}", e);
                    process::exit(0);
                }
            });
            result.0
        }
        Err(e) => {
            println!("Could not connect to PSQL server: {}", e);
            process::exit(1);
        }
    };
    let dataset = Dataset::open(Path::new(gdb_path)).unwrap();
    let nr_of_layers = dataset.layer_count();

    println!("Found {} layers", nr_of_layers);
    let mut index = 0;

    while index < nr_of_layers {
        let mut layer = dataset.layer(index).unwrap();
        let feature_count = layer.feature_count();
        println!(
            "Processing {} layer with {} features",
            layer.name(),
            feature_count
        );
        let field_def = layer
            .defn()
            .fields()
            .map(|field| (field.name(), field.field_type()))
            .collect::<Vec<_>>();

        let mapping = layer_mapping(field_def);

        let layer_name = layer.name();

        let layer_resume = match resume.get(&layer_name) {
            Some(v) => v,
            None => &0,
        };

        if layer_resume == &0 {
            match create_elastic_index(
                &mapping,
                config.get("ELASTIC_HOST").unwrap(),
                &layer_name.to_lowercase(),
                &es_auth,
            )
            .await
            {
                Ok(_) => println!("Created ES Index: {}", layer_name.to_lowercase()),
                Err(e) => {
                    println!("Cannot create elastic index: {}", e);
                    process::exit(1);
                }
            }

            match create_psql_table(&client, &mapping, &layer_name.to_lowercase()).await {
                Ok(_res) => println!("Created PSQL Table: {}", &layer_name.to_lowercase()),
                Err(e) => {
                    println!("Error creating PSQL Table: {:?}", e);
                    process::exit(0);
                }
            };
        }

        let mut chunk: Vec<Feature> = Vec::new();
        let mut iterations = 0;

        for mut feature in layer.features() {
            if geometry_tolerance > 0 as f64 {
                feature
                    .set_geometry(feature.geometry().simplify(geometry_tolerance).unwrap())
                    .unwrap();
            }
            chunk.push(feature);
            if chunk.len() == chunk_size as usize {
                iterations = iterations + 1;

                if layer_resume > &0 && layer_resume >= &((iterations * chunk_size) as i64) {
                    chunk = Vec::new();
                    continue;
                }
                let start = Instant::now();

                match tokio::try_join!(
                    psql_insert(&client, &chunk, &mapping, layer_name.to_lowercase()),
                    elastic_insert(
                        &chunk,
                        &mapping,
                        config.get("ELASTIC_HOST").unwrap(),
                        layer_name.to_lowercase(),
                        &es_auth,
                    )
                ) {
                    Ok((_psql, _es)) => {
                        // do something with the values
                    }
                    Err(err) => {
                        println!("processing failed; error = {}", err);
                        process::exit(0);
                    }
                };
                let remaining_nr_rows = feature_count - (chunk.len() as u64 * iterations as u64);
                let time_per_row = (start.elapsed().as_millis() as f64) / chunk.len() as f64;
                let time_remaining = remaining_nr_rows as f64 * time_per_row / 60000 as f64;
                print!("\u{8}\u{8}\u{8}\u{8}\u{8}\u{8}\u{8}\u{8}\u{8}\u{8}\u{8}\u{8}\u{8}\u{8}\u{8}\u{8}\u{8}\u{8}\u{8}\u{8}\u{8}\u{8}");
                print!("ETA: {:.0?} minutes", time_remaining);
                io::stdout().flush().unwrap();
                chunk = Vec::new();
            }
        }

        if chunk.len() > 0 {
            match tokio::try_join!(
                elastic_insert(
                    &chunk,
                    &mapping,
                    config.get("ELASTIC_HOST").unwrap(),
                    layer_name.to_lowercase(),
                    &es_auth,
                ),
                psql_insert(&client, &chunk, &mapping, layer_name.to_lowercase())
            ) {
                Ok((_psql, _es)) => {
                    // do something with the values
                }
                Err(err) => {
                    println!("processing failed; error = {}", err);
                    process::exit(0);
                }
            };
        }

        let psql_rows = match psql_rows(&client, &layer_name.to_lowercase()).await {
            Ok(rows) => rows,
            Err(e) => {
                println!("Error grabbing ES nr of inserted rows: {}", e);
                0
            }
        };
        let es_rows = match es_rows(
            &config.get("ELASTIC_HOST").unwrap(),
            &layer_name.to_lowercase(),
            &es_auth,
        )
        .await
        {
            Ok(rows) => rows,
            Err(e) => {
                println!("Error grabbing ES nr of inserted rows: {}", e);
                0
            }
        };
        println!(
            "Features: {}; ES:{}; PSQL:{}",
            feature_count, es_rows, psql_rows
        );

        index = index + 1;
    }
}

async fn psql_rows(client: &Client, table: &str) -> Result<i64, Error> {
    match client
        .query(&["SELECT COUNT(*) as count from", table].join(" "), &[])
        .await
    {
        Ok(res) => Ok(res[0].get::<usize, i64>(0)),
        Err(e) => Err(e),
    }
}

async fn es_rows(
    es_host: &str,
    index: &str,
    auth: &HashMap<String, String>,
) -> Result<i64, anyhow::Error> {
    let client = reqwest::Client::new();
    match client
        .get([es_host, "/", index, "/_search"].join(""))
        .body(json!({"size": 0}).to_string())
        .basic_auth(
            auth.get("username").unwrap_or(&"".to_string()),
            Some(auth.get("password").unwrap_or(&"".to_string())),
        )
        .header("content-type", "application/json")
        .send()
        .await
    {
        reqwest::Result::Ok(e) => {
            let text = e.text().await.unwrap();
            let json: Value = serde_json::from_str(&text).unwrap_or_else(|e| {
                println!("Error parsing ES response: {}", e);
                process::exit(1);
            });
            Ok(json["hits"]["total"].as_i64().unwrap_or(0))
        }
        reqwest::Result::Err(e) => Err(anyhow::anyhow!("Request Error {}", e)),
    }
}

async fn psql_insert(
    client: &Client,
    features: &Vec<Feature<'_>>,
    mapping: &HashMap<String, String>,
    table: String,
) -> Result<bool, anyhow::Error> {
    let mut insert_query = Vec::new();

    insert_query.push(["INSERT INTO", &table, "("].join(" "));

    insert_query.push(
        mapping
            .keys()
            .map(|k| k.to_string())
            .collect::<Vec<String>>()
            .join(","),
    );
    insert_query.push(",objectid, the_geom) VALUES ".to_string());

    let mut insert_values = Vec::new();

    for feature in features {
        let mut values: Vec<String> = Vec::new();
        insert_values.push("(".to_string());

        for (key, _value) in mapping {
            match feature.field(key).unwrap() {
                Some(value) => match value {
                    FieldValue::Integer64Value(v) => values.push(v.to_string()),
                    FieldValue::DateTimeValue(v) => {
                        values.push(["'", &v.format("%Y-%m-%d").to_string(), "'"].join(""))
                    }
                    FieldValue::IntegerValue(v) => values.push(v.to_string()),
                    FieldValue::IntegerListValue(v) => values.push(
                        v.into_iter()
                            .map(|v| v.to_string())
                            .collect::<Vec<String>>()
                            .join(""),
                    ),
                    FieldValue::Integer64ListValue(v) => values.push(
                        v.into_iter()
                            .map(|v| v.to_string())
                            .collect::<Vec<String>>()
                            .join(""),
                    ),
                    FieldValue::StringValue(v) => {
                        values.push(["'", &v.replace("'", "''"), "'"].join(""))
                    }
                    FieldValue::StringListValue(v) => {
                        values.push(v.into_iter().map(|v| v).collect::<Vec<String>>().join(""))
                    }
                    FieldValue::RealValue(v) => values.push(v.to_string()),
                    FieldValue::RealListValue(v) => values.push(
                        v.into_iter()
                            .map(|v| v.to_string())
                            .collect::<Vec<String>>()
                            .join(""),
                    ),
                    FieldValue::DateValue(v) => values.push(["'", &v.to_string(), "'"].join("")),
                },
                None => values.push(String::from("NULL")),
            };
        }
        values.push(feature.fid().unwrap().to_string());
        values.push(
            [
                "st_geomfromtext('",
                &feature.geometry().wkt().unwrap(),
                "',4326)",
            ]
            .join(""),
        );
        insert_values.push(values.join(","));
        insert_values.push("),".to_string());
    }

    insert_query
        .push(insert_values.join("").to_string()[0..insert_values.join("").len() - 1].to_string());

    match client.execute(&insert_query.join(" "), &[]).await {
        Ok(_) => Ok(true),
        Err(e) => Err(anyhow::anyhow!(e)),
    }
}

async fn create_psql_table(
    client: &Client,
    mapping: &HashMap<String, String>,
    table: &str,
) -> Result<bool, Error> {
    match client
        .execute(&["DROP TABLE IF EXISTS", table].join(" "), &[])
        .await
    {
        Ok(_) => println!("Dropped table: {}", table),
        Err(e) => {
            println!("Error Dropping existing table {} : {}", table, e);
            process::exit(0);
        }
    };

    let mut table_sql = Vec::new();
    table_sql.push(["CREATE TABLE", table, "("].join(" "));

    for (key, value) in mapping {
        match value.as_str() {
            "string" => table_sql.push([key, "VARCHAR ( 250 ),"].join(" ")),
            "integer" => table_sql.push([key, "int8,"].join(" ")),
            "double" => table_sql.push([key, "float8,"].join(" ")),
            "date" => table_sql.push([key, "date,"].join(" ")),
            &_ => {}
        }
    }

    table_sql.push(["objectid", "int8", "PRIMARY KEY,"].join(" "));
    table_sql.push(["the_geom", "geometry"].join(" "));
    table_sql.push(")".to_string());

    match client.execute(&table_sql.join(" "), &[]).await {
        Ok(_) => Ok(true),
        Err(e) => Err(e),
    }
}

async fn elastic_insert(
    features: &Vec<Feature<'_>>,
    mapping: &HashMap<String, String>,
    es_host: &str,
    index: String,
    auth: &HashMap<String, String>,
) -> Result<bool, anyhow::Error> {
    let body: Vec<String> = process_rows(features, mapping, &index);

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
                println!("Error parsing ES response: {}", e);
                process::exit(1);
            });
            if json["errors"].as_bool().unwrap_or(false) {
                let mut errors: i32 = 0;
                for items in json["items"].as_array() {
                    for item in items {
                        if item["index"]["status"] == 400 {
                            errors = errors + 1;
                        }
                    }
                }

                if errors > 0 {
                    println!("Could not insert {} rows", errors);
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
        es_row["objectid"] = Value::from(feature.fid().unwrap());
        es_row["the_geom"] =
            serde_json::from_str(&feature.geometry().json().unwrap().replace("\\\"", "\""))
                .unwrap();

        body.push([json!(es_row).to_string(), "\n".to_string()].join(""));
    }
    body
}

async fn psql_connect(
    config: &Properties,
) -> Result<(Client, Connection<Socket, NoTlsStream>), Error> {
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

    match tokio_postgres::connect(&connection_string, NoTls).await {
        Ok(res) => Ok(res),
        Err(e) => Err(e),
    }
}

async fn create_elastic_index(
    mapping: &HashMap<String, String>,
    es_host: &str,
    index: &str,
    auth: &HashMap<String, String>,
) -> Result<bool, reqwest::Error> {
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
                return Ok(true);
            }
            Ok(false)
        }
        reqwest::Result::Err(e) => reqwest::Result::Err(e),
    }
}

fn create_elastic_mapping(mapping: &HashMap<String, String>) -> Value {
    let mut es_mapping = json!({
        "settings": {
            "index": {
                "refresh_interval": "1s",
                "number_of_shards": 4,
                "number_of_replicas": 0,
                "max_result_window" : 500000000
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
                    "objectid":{
                        "type": "integer"
                    },
                    "the_geom":{
                        "type": "geo_shape"
                    }
                }
            }
        }
    });

    for (key, value) in mapping {
        let lowercase_key = key.to_lowercase();
        match value.as_str() {
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
        match field.1 .1 {
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
            println!("Missing Config for: {}", entry);
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
