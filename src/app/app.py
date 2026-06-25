from flask import Flask
from flask import render_template
from flask import jsonify

from services.dashboard_service import (
    load_dashboard
)

app = Flask(
    __name__,
    static_folder="static",
    template_folder="templates"
)


@app.route("/")
def index():

    dashboard = load_dashboard()

    return render_template(
        "index.html",
        summary=dashboard["summary"],
        regions=dashboard["regions"],
        insights=dashboard["insights"]
    )


@app.route("/api/dashboard")
def api_dashboard():

    return jsonify(
        load_dashboard()
    )


from flask import send_from_directory


@app.route("/output/<path:file>")
def output(file):

    response = send_from_directory(
        "/app/output",
        file
    )

    if file.endswith(".html"):

        response.headers[
            "X-Frame-Options"
        ] = "ALLOWALL"

    return response

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=True
    )
