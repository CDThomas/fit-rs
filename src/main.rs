use async_graphql::{
    http::{playground_source, GraphQLPlaygroundConfig},
    Context, EmptyMutation, EmptySubscription, Object, Schema, SimpleObject,
};
use tide::{http::mime, Body, Response, StatusCode};

use diesel::prelude::*;
use fit::models::*;

#[derive(SimpleObject)]
pub struct Demo {
    pub field: String,
}

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn demo(&self, _ctx: &Context<'_>) -> Demo {
        use fit::schema::posts::dsl::*;

        // TODO: move connections into ctx
        let connection = fit::establish_connection();

        let post = posts
            .first::<Post>(&connection)
            .expect("Error loading posts");

        Demo { field: post.title }
    }
}

#[async_std::main]
async fn main() -> tide::Result<()> {
    let mut app = tide::new();

    // create schema
    let schema = Schema::build(QueryRoot, EmptyMutation, EmptySubscription).finish();

    // add tide endpoint
    app.at("/graphql")
        .post(async_graphql_tide::endpoint(schema));

    // enable graphql playground
    app.at("/").get(|_| async move {
        Ok(Response::builder(StatusCode::Ok)
            .body(Body::from_string(playground_source(
                // note that the playground needs to know
                // the path to the graphql endpoint
                GraphQLPlaygroundConfig::new("/graphql"),
            )))
            .content_type(mime::HTML)
            .build())
    });

    Ok(app.listen("127.0.0.1:8080").await?)
}
