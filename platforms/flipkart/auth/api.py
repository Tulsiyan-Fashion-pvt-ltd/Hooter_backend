import secrets
from quart import Blueprint, jsonify, redirect, request, session
from utils.prerequirements import login_required, brand_required
from ..stores import mariadb
from platforms.flipkart.auth.helper import (
    generate_state,
    build_flipkart_authorization_url,
    exchange_authorization_code,
)

auth = Blueprint("flipkart_auth", __name__, url_prefix="/auth")


@auth.get("/authorize")
@login_required
@brand_required
async def authorize_flipkart():
    """
    Start Flipkart OAuth authorization for the currently selected brand.
    Each brand can connect only one Flipkart account.
    """
    brand_id = session.get("brand")

    if brand_id is None:
        return jsonify({"status": "error", "message": "Brand ID is missing from the session",}), 400

    try:
        brand_id = int(brand_id)
    except (TypeError, ValueError):
        return jsonify({"status": "error", "message": "Invalid brand ID",}), 400

    # Check whether this brand already has a Flipkart account.
    existing_account = await mariadb.Fetch.get_account(brand_id)

    if existing_account:
        return jsonify({"status": "error", "message": "This brand already has a Flipkart account connected",}), 409

    # Generate state 
    state = generate_state()
    session["flipkart_oauth_state"] = state

    try:
        authorization_url = build_flipkart_authorization_url(state)
    except Exception:
        return jsonify({"status": "error","message": "Unable to start Flipkart authorization",}), 500

    return redirect(authorization_url)


@auth.get("/callback")
@login_required
@brand_required
async def flipkart_callback():
    """
    Handle Flipkart's OAuth callback and save tokens for the current brand.
    """
    code = request.args.get("code")
    returned_state = request.args.get("state")

    if not code or not returned_state:
        return jsonify({"status": "error","message": "Missing authorization code or state",}), 400

    expected_state = session.get("flipkart_oauth_state")

    if (not expected_state or not secrets.compare_digest
        (
            str(returned_state),
            str(expected_state),
        )
    ):
        return jsonify({"status": "error", "message": "Invalid or missing OAuth state",}), 403

    session.pop("flipkart_oauth_state", None)

    brand_id = session.get("brand")

    if brand_id is None:
        return jsonify({"status": "error", "message": "Brand ID is missing from the session",}), 400

    try:
        brand_id = int(brand_id)
    except (TypeError, ValueError):
        return jsonify({"status": "error", "message": "Invalid brand ID",}), 400

    try:
        token_data = await exchange_authorization_code(code=code, state=returned_state,)
    except Exception:
        return jsonify({"status": "error", "message": "Unable to exchange the Flipkart authorization code",}), 502

    access_token = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token")
    expires_in = token_data.get("expires_in")
    refresh_token_expires_in = token_data.get("refresh_token_expires_in")
    scope = token_data.get("scope")

    if not access_token or not refresh_token or expires_in is None:
        return jsonify({"status": "error", "message": "Flipkart returned an incomplete token response",}), 502

    try:
        expires_in = int(expires_in)

        if expires_in <= 0:
            raise ValueError

        if refresh_token_expires_in is not None:
            refresh_token_expires_in = int(refresh_token_expires_in)

            if refresh_token_expires_in <= 0:
                raise ValueError

    except (TypeError, ValueError):
        return jsonify({"status": "error", "message": "Flipkart returned invalid token expiry values",}), 502

    result = await mariadb.Write.add_account(
        brand_id=brand_id,
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=expires_in,
        refresh_token_expires_in=refresh_token_expires_in,
        scope=scope,
    )

    if result.get("status") != "ok":
        return jsonify({
            "status": "error",
            "message": result.get(
                "message",
                "Unable to save the Flipkart account",
            ),
        }), 409

    return jsonify({"status": "success", "message": "Flipkart account connected successfully", "brand_id": brand_id,}), 200