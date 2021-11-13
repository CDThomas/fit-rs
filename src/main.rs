use async_graphql::dataloader::{DataLoader, Loader};
use async_graphql::futures_util::TryStreamExt;
use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql::{Context, EmptySubscription, FieldError, Object, Result, Schema, SimpleObject};
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

#[derive(sqlx::FromRow, Clone, SimpleObject)]
pub struct Routine {
    id: i32,
    name: String,
}

pub struct RoutineLoader(Pool<Postgres>);

impl RoutineLoader {
    fn new(postgres_pool: Pool<Postgres>) -> Self {
        Self(postgres_pool)
    }
}

#[async_trait]
impl Loader<i32> for RoutineLoader {
    type Value = Routine;
    type Error = FieldError;

    async fn load(&self, keys: &[i32]) -> Result<HashMap<i32, Self::Value>, Self::Error> {
        let query = "SELECT id, name FROM routines WHERE id IN (SELECT * FROM UNNEST($1))";
        let exercise = sqlx::query_as(&query)
            .bind(keys)
            .fetch(&self.0)
            .map_ok(|routine: Routine| (routine.id, routine))
            .try_collect()
            .await?;

        Ok(exercise)
    }
}

struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn exercises(&self, ctx: &Context<'_>) -> Result<Vec<Exercise>> {
        let pool = ctx.data_unchecked::<sqlx::Pool<sqlx::Postgres>>();

        let exercises = sqlx::query_as!(Exercise, "SELECT id, name FROM exercises")
            .fetch(pool)
            .try_collect()
            .await?;

        Ok(exercises)
    }

    async fn routine(&self, ctx: &Context<'_>, id: i32) -> Result<Option<Routine>> {
        let routine = ctx
            .data_unchecked::<DataLoader<RoutineLoader>>()
            .load_one(id)
            .await?;

        Ok(routine)
    }

    async fn routines(&self, ctx: &Context<'_>) -> Result<Vec<Routine>> {
        let pool = ctx.data_unchecked::<sqlx::Pool<sqlx::Postgres>>();

        let routines = sqlx::query_as!(Routine, "SELECT id, name FROM routines")
            .fetch(pool)
            .try_collect()
            .await?;

        Ok(routines)
    }
}

struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn create_routine(&self, ctx: &Context<'_>, name: String) -> Result<Routine> {
        let pool = ctx.data_unchecked::<sqlx::Pool<sqlx::Postgres>>();

        let routine = sqlx::query_as!(
            Routine,
            "INSERT INTO routines (name) VALUES ( $1 ) RETURNING id, name",
            name
        )
        .fetch_one(pool)
        .await?;

        Ok(routine)
    }
}

fn main() -> Result<()> {
    task::block_on(run())
}

async fn run() -> Result<()> {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set in env");
    let postgres_pool: Pool<Postgres> = Pool::connect(&database_url).await?;

    let schema = Schema::build(QueryRoot, MutationRoot, EmptySubscription)
        .data(DataLoader::new(RoutineLoader::new(postgres_pool.clone())))
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
