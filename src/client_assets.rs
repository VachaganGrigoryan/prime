use axum::{Router, http::header, response::Html, routing::get};

pub fn icon_router() -> Router {
    let mut router = Router::new();
    for (path, content_type, body) in [
        (
            "/favicon.ico",
            "image/vnd.microsoft.icon",
            include_bytes!("client_assets/favicon.ico").as_slice(),
        ),
        (
            "/favicon-96.png",
            "image/png",
            include_bytes!("client_assets/favicon-96.png").as_slice(),
        ),
        (
            "/favicon.svg",
            "image/svg+xml",
            include_bytes!("client_assets/favicon.svg").as_slice(),
        ),
        (
            "/apple-touch-icon.png",
            "image/png",
            include_bytes!("client_assets/apple-touch-icon.png").as_slice(),
        ),
    ] {
        router = router.route(path, get(([(header::CONTENT_TYPE, content_type)], body)));
    }
    router
}

pub fn router() -> Router {
    let dashboard = icon_router()
        .route(
            "/social-card.png",
            get((
                [(header::CONTENT_TYPE, "image/png")],
                include_bytes!("client_assets/social-card.png").as_slice(),
            )),
        )
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
            "/robots.txt",
            "text/plain; charset=utf-8",
            include_str!("client_assets/robots.txt"),
        ),
        (
            "/sitemap.xml",
            "application/xml; charset=utf-8",
            include_str!("client_assets/sitemap.xml"),
        ),
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
