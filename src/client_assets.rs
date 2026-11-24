use axum::{Router, http::header, response::Html, routing::get};

pub fn router() -> Router {
    let dashboard = Router::new()
        .route(
            "/",
            get(|| async { Html(include_str!("client_assets/index.html")) }),
        )
        .route(
            "/research.html",
            get(|| async { Html(include_str!("client_assets/research.html")) }),
        )
        .route(
            "/docs.html",
            get(|| async { Html(include_str!("client_assets/docs.html")) }),
        )
        .route(
            "/style.css",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
                    include_str!("client_assets/style.css"),
                )
            }),
        )
        .route(
            "/app.js",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
                    include_str!("client_assets/app.js"),
                )
            }),
        );

    let mut router = dashboard.route(
        "/estimates.html",
        get(|| async { Html(include_str!("client_assets/estimates.html")) }),
    );
    for (path, content_type, body) in [
        (
            "/estimates.css",
            "text/css; charset=utf-8",
            include_str!("client_assets/estimates.css"),
        ),
        (
            "/estimates.js",
            "text/javascript; charset=utf-8",
            include_str!("client_assets/estimates.js"),
        ),
        (
            "/prime-experiment.json",
            "application/json",
            include_str!("client_assets/prime-experiment.json"),
        ),
        (
            "/prime-score.json",
            "application/json",
            include_str!("client_assets/prime-score.json"),
        ),
        (
            "/prime-prediction.json",
            "application/json",
            include_str!("client_assets/prime-prediction.json"),
        ),
    ] {
        router = router.route(
            path,
            get(move || async move { ([(header::CONTENT_TYPE, content_type)], body) }),
        );
    }
    router
}
