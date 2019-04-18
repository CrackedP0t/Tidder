use askama::Template;
use gotham::helpers::http::response::{create_empty_response, create_response};
use gotham::state::State;
use hyper::{Body, Response, StatusCode};
use tokio_postgres::{self, NoTls};

pub const MESSAGE: &str = "Hello, Gotham!";

/// The index displays a message to the browser.
/// The default template directory is `$CRATE_ROOT/templates`,which is what we are using in this example
#[derive(Debug, Template)]
#[template(path = "index.html")]
pub struct Index {
    pub message: String,
}

/// Renders the `index.html` template with the `MESSAGE` constant as the message
pub fn index(state: State) -> (State, Response<Body>) {
    let tpl = Index {
        message: MESSAGE.to_string(),
    };

    // The response is either the rendered template, or a server error if something really goes wrong
    let res = match tpl.render() {
        Ok(content) => create_response(
            &state,
            StatusCode::OK,
            mime::TEXT_HTML_UTF_8,
            content.into_bytes(),
        ),
        Err(_) => create_empty_response(&state, StatusCode::INTERNAL_SERVER_ERROR),
    };

    (state, res)
}

/// Run on the normal port for Gotham examples, passing the handler as the only function for the gotham web server.
pub fn main() {
    tokio_postgres::connect("postgres://postgres@%2Frun%2Fpostgresql/tidder", NoTls);
    let addr = "127.0.0.1:7878";
    println!("Listening at {}", addr);
    gotham::start(addr, || Ok(index));
}
