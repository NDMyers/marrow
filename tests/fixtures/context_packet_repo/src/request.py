def handle_request(request):
    parsed = parse_request(request)
    if authorize_request(parsed):
        return build_response(parsed)
    return build_response({"error": "denied"})


def parse_request(request):
    return {"path": request.get("path", "/"), "user": request.get("user")}


def authorize_request(parsed):
    return parsed.get("user") is not None


def build_response(parsed):
    return {"status": 200, "body": parsed}