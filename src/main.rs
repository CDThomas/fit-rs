use async_graphql::dataloader::{DataLoader, Loader};
use async_graphql::futures_util::TryStreamExt;
use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql::{
    Context, EmptyMutation, EmptySubscription, FieldError, Object, Result, Schema, SimpleObject,
};
use async_std::task;
use async_trait::async_trait;
use sqlx::{Pool, Postgres};
use std::collections::HashMap;
use std::env;
use tide::{http::mime, Body, Response, StatusCode};

#[derive(sqlx::FromRow, Clone, SimpleObject)]
pub struct Exercise {
    id: i32,
    name: String,
}

pub struct ExerciseLoader(Pool<Postgres>);

impl ExerciseLoader {
    fn new(postgres_pool: Pool<Postgres>) -> Self {
        Self(postgres_pool)
    }
}

#[async_trait]
impl Loader<i32> for ExerciseLoader {
    type Value = Exercise;
    type Error = FieldError;

    async fn load(&self, keys: &[i32]) -> Result<HashMap<i32, Self::Value>, Self::Error> {
        println!("load exercise by batch {:?}", keys);

        let query = "SELECT id, name FROM exercises WHERE id IN (SELECT * FROM UNNEST($1))";
        let exercise = sqlx::query_as(&query)
            .bind(keys)
            .fetch(&self.0)
            .map_ok(|exercise: Exercise| (exercise.id, exercise))
            .try_collect()
            .await?;

        Ok(exercise)
    }
}

struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn exercise(&self, ctx: &Context<'_>, id: i32) -> Result<Option<Exercise>> {
        let exercise = ctx
            .data_unchecked::<DataLoader<ExerciseLoader>>()
            .load_one(id)
            .await?;

        Ok(exercise)
    }

    async fn exercises(&self, ctx: &Context<'_>) -> Result<Vec<Exercise>> {
        let pool = ctx.data_unchecked::<sqlx::Pool<sqlx::Postgres>>();

        let exercises = sqlx::query_as!(Exercise, "SELECT id, name FROM exercises")
            .fetch(pool)
            .try_collect()
            .await?;

        Ok(exercises)
    }
}

fn main() -> Result<()> {
    task::block_on(run())
}

async fn run() -> Result<()> {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set in env");
    let postgres_pool: Pool<Postgres> = Pool::connect(&database_url).await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS exercises (
            id INTEGER PRIMARY KEY NOT NULL,
            name TEXT NOT NULL
        );
        "#,
    )
    .execute(&postgres_pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO exercises (id, name)
        VALUES (1, 'Squat'), (2, 'Deadlift'), (3, 'Row')
        ON CONFLICT (id) DO NOTHING;
        "#,
    )
    .execute(&postgres_pool)
    .await?;

    let schema = Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(DataLoader::new(ExerciseLoader::new(postgres_pool.clone())))
        .data(postgres_pool.clone())
        .finish();

    let mut app = tide::new();

    app.at("/graphql")
        .post(async_graphql_tide::endpoint(schema));

    app.at("/").get(|_| async move {
        let mut resp = Response::new(StatusCode::Ok);
        resp.set_body(Body::from_string(playground_source(
            GraphQLPlaygroundConfig::new("/graphql"),
        )));
        resp.set_content_type(mime::HTML);
        Ok(resp)
    });

    println!("Playground: http://127.0.0.1:8000");
    app.listen("127.0.0.1:8000").await?;

    Ok(())
}
