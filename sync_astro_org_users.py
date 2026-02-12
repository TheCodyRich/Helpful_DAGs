"""
DAG that syncs users, roles, and permissions between two Astro Organizations. 

Syncs at five levels: Custom Roles, Organization, Team, Workspace,
and Deployment.
Masters Org (IDP source) → Copy Org (target, one-way).

Sync order: Custom Roles → Org users → Teams → Workspace users
→ Deployment roles.
(Custom roles synced first so deployment role references are valid.
Teams synced before workspaces due to role inheritance.
Deployment roles synced last since they reference deployments in
workspaces that must already exist in the copy org.)

Unmatched workspaces/deployments are skipped.
Teams/workspaces/deployments matched by name.

### Required Environment Variables
- MASTER_ORG_ID, COPY_ORG_ID, MASTER_API_TOKEN, COPY_API_TOKEN
"""

import os
import logging
from pendulum import datetime

from airflow.sdk import dag, task

# Environment variables for org configuration
MASTER_ORG_ID = os.getenv("MASTER_ORG_ID")
COPY_ORG_ID = os.getenv("COPY_ORG_ID")
MASTER_API_TOKEN = os.getenv("MASTER_API_TOKEN")
COPY_API_TOKEN = os.getenv("COPY_API_TOKEN")

ASTRO_API_BASE_URL = "https://api.astronomer.io"

task_logger = logging.getLogger("airflow.task")


def make_api_request(method, url, headers, json_data=None, params=None):
    """Helper function to make API requests with error handling."""
    import requests

    try:
        response = requests.request(
            method=method, url=url, headers=headers, json=json_data, params=params
        )
        response.raise_for_status()
        return response.json() if response.text else {}
    except requests.exceptions.RequestException:
        raise


def paginate_api_get(url, headers, key, params=None):
    """Helper to paginate through API results."""
    import requests

    all_items = []
    offset = 0
    limit = 100
    params = params or {}

    while True:
        params.update({"offset": offset, "limit": limit})
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        items = data.get(key, [])
        all_items.extend(items)

        if len(items) < limit:
            break
        offset += limit

    return all_items


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",  # Every 5 minutes
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 2},
    tags=["astro", "sync", "users", "permissions"],
)
def sync_astro_org_users():
    @task
    def validate_config() -> dict:
        """Validate that all required environment variables are set."""
        missing = []

        if not MASTER_ORG_ID:
            missing.append("MASTER_ORG_ID")
        if not COPY_ORG_ID:
            missing.append("COPY_ORG_ID")
        if not MASTER_API_TOKEN:
            missing.append("MASTER_API_TOKEN")
        if not COPY_API_TOKEN:
            missing.append("COPY_API_TOKEN")

        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

        task_logger.info("Configuration validated successfully")
        return {
            "master_org_id": MASTER_ORG_ID,
            "copy_org_id": COPY_ORG_ID,
        }

    # =========================================================================
    # CUSTOM ROLES SYNC
    # =========================================================================

    @task
    def sync_custom_roles(config: dict) -> dict:
        """Sync custom roles (DEPLOYMENT-scoped) from master to copy org.

        Ensures the copy org has the same custom deployment roles as
        the master org before deployment-level role assignments are synced.
        Roles are matched by name. New roles are created, existing roles
        are updated if permissions or description differ, and roles in
        the copy org that don't exist in the master are deleted.
        """
        import requests

        master_org_id = config["master_org_id"]
        copy_org_id = config["copy_org_id"]

        master_headers = {
            "Authorization": f"Bearer {MASTER_API_TOKEN}",
            "Content-Type": "application/json",
        }
        copy_headers = {
            "Authorization": f"Bearer {COPY_API_TOKEN}",
            "Content-Type": "application/json",
        }

        roles_created = []
        roles_updated = []
        roles_deleted = []
        errors = []

        # Fetch custom roles from both orgs (paginated, limit 100)
        def fetch_custom_roles(org_id, headers, label):
            """Fetch all custom roles (DEPLOYMENT scope) for an org."""
            url = f"{ASTRO_API_BASE_URL}/v1/organizations/{org_id}/roles"
            all_roles = []
            offset = 0
            limit = 100
            while True:
                params = {
                    "offset": offset,
                    "limit": limit,
                    "scopeTypes": "DEPLOYMENT",
                }
                try:
                    resp = requests.get(url, headers=headers, params=params)
                    resp.raise_for_status()
                    data = resp.json()
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 403:
                        task_logger.warning(
                            f"403 Forbidden: {label} token lacks permission "
                            f"to list custom roles."
                        )
                        return []
                    raise

                roles = data.get("roles", [])
                all_roles.extend(roles)
                if len(roles) < limit:
                    break
                offset += limit

            task_logger.info(
                f"Retrieved {len(all_roles)} custom roles from {label} org"
            )
            return all_roles

        def get_role_details(org_id, role_id, headers):
            """Fetch a single role with its permissions."""
            url = f"{ASTRO_API_BASE_URL}/v1/organizations/{org_id}/roles/{role_id}"
            return make_api_request("GET", url, headers)

        master_roles = fetch_custom_roles(master_org_id, master_headers, "master")
        copy_roles = fetch_custom_roles(copy_org_id, copy_headers, "copy")

        # Enrich roles with permissions (list endpoint doesn't include them)
        master_roles_detail = {}
        for role in master_roles:
            try:
                detail = get_role_details(master_org_id, role["id"], master_headers)
                master_roles_detail[role["name"]] = detail
            except requests.exceptions.RequestException as e:
                error_msg = (
                    f"[ROLES] Failed to get details for master role "
                    f"'{role['name']}': {e}"
                )
                task_logger.error(error_msg)
                errors.append(error_msg)

        copy_roles_detail = {}
        for role in copy_roles:
            try:
                detail = get_role_details(copy_org_id, role["id"], copy_headers)
                copy_roles_detail[role["name"]] = detail
            except requests.exceptions.RequestException as e:
                error_msg = (
                    f"[ROLES] Failed to get details for copy role '{role['name']}': {e}"
                )
                task_logger.error(error_msg)
                errors.append(error_msg)

        # Remap workspace restrictions: master ws IDs → copy ws IDs by name
        master_ws_url = (
            f"{ASTRO_API_BASE_URL}/v1/organizations/{master_org_id}/workspaces"
        )
        copy_ws_url = f"{ASTRO_API_BASE_URL}/v1/organizations/{copy_org_id}/workspaces"
        try:
            master_wss = paginate_api_get(master_ws_url, master_headers, "workspaces")
        except requests.exceptions.RequestException:
            master_wss = []
        try:
            copy_wss = paginate_api_get(copy_ws_url, copy_headers, "workspaces")
        except requests.exceptions.RequestException:
            copy_wss = []

        master_ws_id_to_name = {ws["id"]: ws["name"] for ws in master_wss}
        copy_ws_name_to_id = {ws["name"]: ws["id"] for ws in copy_wss}

        def remap_restricted_ws_ids(master_ws_ids):
            """Translate master workspace IDs to copy workspace IDs."""
            remapped = []
            for ws_id in master_ws_ids or []:
                ws_name = master_ws_id_to_name.get(ws_id)
                if ws_name and ws_name in copy_ws_name_to_id:
                    remapped.append(copy_ws_name_to_id[ws_name])
                elif ws_name:
                    task_logger.info(
                        f"[ROLES] Skipping workspace restriction "
                        f"'{ws_name}' (not in copy org)"
                    )
            return remapped

        # Create missing roles in copy org
        for name, m_role in master_roles_detail.items():
            if name not in copy_roles_detail:
                try:
                    url = f"{ASTRO_API_BASE_URL}/v1/organizations/{copy_org_id}/roles"
                    remapped_ws = remap_restricted_ws_ids(
                        m_role.get("restrictedWorkspaceIds", [])
                    )
                    payload = {
                        "name": m_role["name"],
                        "permissions": m_role.get("permissions", []),
                        "scopeType": m_role.get("scopeType", "DEPLOYMENT"),
                        "description": m_role.get("description", ""),
                    }
                    if remapped_ws:
                        payload["restrictedWorkspaceIds"] = remapped_ws

                    result = make_api_request(
                        "POST", url, copy_headers, json_data=payload
                    )
                    roles_created.append(
                        {
                            "name": name,
                            "id": result.get("id"),
                            "level": "CUSTOM_ROLE",
                        }
                    )
                    task_logger.info(
                        f"[ROLES] ✓ CREATE: '{name}' (scope: {m_role.get('scopeType')})"
                    )
                except requests.exceptions.RequestException as e:
                    error_msg = f"[ROLES] ✗ CREATE FAILED for '{name}': {e}"
                    task_logger.error(error_msg)
                    errors.append(error_msg)

        # Update existing roles if permissions/description differ
        for name, m_role in master_roles_detail.items():
            if name in copy_roles_detail:
                c_role = copy_roles_detail[name]
                m_perms = sorted(m_role.get("permissions", []))
                c_perms = sorted(c_role.get("permissions", []))
                m_desc = m_role.get("description", "")
                c_desc = c_role.get("description", "")
                m_ws = sorted(
                    remap_restricted_ws_ids(m_role.get("restrictedWorkspaceIds", []))
                )
                c_ws = sorted(c_role.get("restrictedWorkspaceIds", []))

                if m_perms != c_perms or m_desc != c_desc or m_ws != c_ws:
                    try:
                        url = (
                            f"{ASTRO_API_BASE_URL}/v1/organizations/"
                            f"{copy_org_id}/roles/{c_role['id']}"
                        )
                        payload = {
                            "name": name,
                            "permissions": m_role.get("permissions", []),
                            "description": m_desc,
                        }
                        if m_ws:
                            payload["restrictedWorkspaceIds"] = m_ws

                        make_api_request("POST", url, copy_headers, json_data=payload)
                        changes = []
                        if m_perms != c_perms:
                            changes.append("permissions")
                        if m_desc != c_desc:
                            changes.append("description")
                        if m_ws != c_ws:
                            changes.append("workspace restrictions")
                        roles_updated.append(
                            {
                                "name": name,
                                "change": ", ".join(changes),
                                "level": "CUSTOM_ROLE",
                            }
                        )
                        task_logger.info(
                            f"[ROLES] ✓ UPDATE: '{name}' ({', '.join(changes)})"
                        )
                    except requests.exceptions.RequestException as e:
                        error_msg = f"[ROLES] ✗ UPDATE FAILED for '{name}': {e}"
                        task_logger.error(error_msg)
                        errors.append(error_msg)

        # Delete roles in copy org that are not in master
        for name, c_role in copy_roles_detail.items():
            if name not in master_roles_detail:
                try:
                    url = (
                        f"{ASTRO_API_BASE_URL}/v1/organizations/"
                        f"{copy_org_id}/roles/{c_role['id']}"
                    )
                    resp = requests.delete(url, headers=copy_headers)
                    if resp.status_code == 404:
                        task_logger.info(
                            f"[ROLES] ✓ DELETE: '{name}' already gone (404)"
                        )
                    else:
                        resp.raise_for_status()
                        task_logger.info(f"[ROLES] ✓ DELETE: '{name}'")
                    roles_deleted.append({"name": name, "level": "CUSTOM_ROLE"})
                except requests.exceptions.RequestException as e:
                    error_msg = f"[ROLES] ✗ DELETE FAILED for '{name}': {e}"
                    task_logger.error(error_msg)
                    errors.append(error_msg)

        return {
            "roles_created": roles_created,
            "roles_updated": roles_updated,
            "roles_deleted": roles_deleted,
            "errors": errors,
        }

    # =========================================================================
    # ORGANIZATION LEVEL SYNC
    # =========================================================================

    @task
    def get_master_org_users(config: dict) -> list[dict]:
        """Fetch all users and roles from master org."""
        import requests

        org_id = config["master_org_id"]
        url = f"{ASTRO_API_BASE_URL}/v1/organizations/{org_id}/users"
        headers = {
            "Authorization": f"Bearer {MASTER_API_TOKEN}",
            "Content-Type": "application/json",
        }

        try:
            all_users = paginate_api_get(url, headers, "users")
            task_logger.info(f"Retrieved {len(all_users)} users from master org")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                task_logger.error(
                    "403 Forbidden: MASTER_API_TOKEN lacks permission to list org users. "
                    "Ensure the token has ORGANIZATION_OWNER role and belongs to the correct org. "
                    f"Org ID: {org_id}"
                )
                return []
            raise

        return [
            {
                "email": user.get("username", user.get("email", "")),
                "fullName": user.get("fullName", ""),
                "id": user.get("id"),
                "role": user.get("organizationRole", "ORGANIZATION_MEMBER"),
                "idpManaged": user.get("idpManaged", False),
            }
            for user in all_users
        ]

    @task
    def get_copy_org_users(config: dict) -> list[dict]:
        """Fetch all users and roles from copy org."""
        import requests

        org_id = config["copy_org_id"]
        url = f"{ASTRO_API_BASE_URL}/v1/organizations/{org_id}/users"
        headers = {
            "Authorization": f"Bearer {COPY_API_TOKEN}",
            "Content-Type": "application/json",
        }

        try:
            all_users = paginate_api_get(url, headers, "users")
            task_logger.info(f"Retrieved {len(all_users)} users from copy org")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                task_logger.error(
                    "403 Forbidden: COPY_API_TOKEN lacks permission to list org users. "
                    "Ensure the token has ORGANIZATION_OWNER role and belongs to the correct org. "
                    f"Org ID: {org_id}"
                )
                return []
            raise

        return [
            {
                "email": user.get("username", user.get("email", "")),
                "fullName": user.get("fullName", ""),
                "id": user.get("id"),
                "role": user.get("organizationRole", "ORGANIZATION_MEMBER"),
                "idpManaged": user.get("idpManaged", False),
            }
            for user in all_users
        ]

    @task
    def sync_org_users(
        config: dict,
        master_users: list[dict],
        copy_users: list[dict],
    ) -> dict:
        """Sync org-level users: invite new, update roles, remove old."""
        import requests

        org_id = config["copy_org_id"]
        headers = {
            "Authorization": f"Bearer {COPY_API_TOKEN}",
            "Content-Type": "application/json",
        }

        master_by_email = {u["email"].lower(): u for u in master_users if u["email"]}
        copy_by_email = {u["email"].lower(): u for u in copy_users if u["email"]}

        users_invited = []
        users_removed = []
        roles_updated = []
        errors = []

        # Invite new users
        for email, master_user in master_by_email.items():
            if email not in copy_by_email:
                try:
                    url = f"{ASTRO_API_BASE_URL}/v1/organizations/{org_id}/invites"
                    payload = {
                        "inviteeEmail": master_user["email"],
                        "role": master_user["role"],
                    }
                    response = requests.post(url, headers=headers, json=payload)
                    response.raise_for_status()
                    users_invited.append(
                        {
                            "email": master_user["email"],
                            "role": master_user["role"],
                            "name": master_user["fullName"],
                            "level": "ORGANIZATION",
                        }
                    )
                    task_logger.info(
                        f"[ORG] ✓ ADD: Invited {master_user['email']} "
                        f"as {master_user['role']}"
                    )
                except requests.exceptions.RequestException as e:
                    error_msg = f"[ORG] ✗ ADD FAILED for {master_user['email']}: {e}"
                    task_logger.error(error_msg)
                    errors.append(error_msg)

        # Remove old users
        for email, copy_user in copy_by_email.items():
            if email not in master_by_email:
                try:
                    url = f"{ASTRO_API_BASE_URL}/v1/organizations/{org_id}/users/{copy_user['id']}/roles"
                    payload = {"organizationRole": None}
                    response = requests.post(url, headers=headers, json=payload)
                    response.raise_for_status()
                    users_removed.append(
                        {
                            "email": copy_user["email"],
                            "name": copy_user["fullName"],
                            "level": "ORGANIZATION",
                        }
                    )
                    task_logger.info(
                        f"[ORG] ✓ REMOVE: Removed {copy_user['email']} from org"
                    )
                except requests.exceptions.RequestException as e:
                    error_msg = f"[ORG] ✗ REMOVE FAILED for {copy_user['email']}: {e}"
                    task_logger.error(error_msg)
                    errors.append(error_msg)

        # Update roles for users in both orgs
        for email, master_user in master_by_email.items():
            if email in copy_by_email:
                copy_user = copy_by_email[email]
                if master_user["role"] != copy_user["role"]:
                    try:
                        user_id = copy_user["id"]
                        url = f"{ASTRO_API_BASE_URL}/v1/organizations/{org_id}/users/{user_id}/roles"
                        payload = {
                            "organizationRole": master_user["role"],
                        }
                        task_logger.info(
                            f"[ORG] ROLE CHANGE: Updating {master_user['email']} "
                            f"from {copy_user['role']} to {master_user['role']}..."
                        )
                        response = requests.post(url, headers=headers, json=payload)
                        response.raise_for_status()
                        roles_updated.append(
                            {
                                "email": master_user["email"],
                                "old_role": copy_user["role"],
                                "new_role": master_user["role"],
                                "name": master_user["fullName"],
                                "level": "ORGANIZATION",
                            }
                        )
                        task_logger.info(
                            f"[ORG] ✓ Role updated for {master_user['email']}: "
                            f"{copy_user['role']} -> {master_user['role']}"
                        )
                    except requests.exceptions.RequestException as e:
                        error_msg = (
                            f"[ORG] ✗ ROLE UPDATE FAILED for {master_user['email']} "
                            f"({copy_user['role']} -> {master_user['role']}): {e}"
                        )
                        task_logger.error(error_msg)
                        errors.append(error_msg)

        return {
            "users_invited": users_invited,
            "users_removed": users_removed,
            "roles_updated": roles_updated,
            "errors": errors,
        }

    # =========================================================================
    # WORKSPACE LEVEL SYNC
    # =========================================================================

    @task
    def get_workspaces(
        config: dict,
        master_users: list[dict],
        copy_users: list[dict],
    ) -> dict:
        """Get workspaces and derive per-workspace user membership from roles."""
        import requests

        master_org_id = config["master_org_id"]
        copy_org_id = config["copy_org_id"]

        master_headers = {
            "Authorization": f"Bearer {MASTER_API_TOKEN}",
            "Content-Type": "application/json",
        }
        copy_headers = {
            "Authorization": f"Bearer {COPY_API_TOKEN}",
            "Content-Type": "application/json",
        }

        master_workspaces = []
        copy_workspaces = []

        # Get master workspaces
        master_url = f"{ASTRO_API_BASE_URL}/v1/organizations/{master_org_id}/workspaces"
        try:
            master_workspaces = paginate_api_get(
                master_url, master_headers, "workspaces"
            )
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                task_logger.warning(
                    "403 Forbidden: MASTER_API_TOKEN lacks permission to list workspaces. "
                    "Ensure the token has ORGANIZATION_OWNER role. "
                    "Skipping workspace-level sync."
                )
            else:
                raise

        # Get copy workspaces
        copy_url = f"{ASTRO_API_BASE_URL}/v1/organizations/{copy_org_id}/workspaces"
        try:
            copy_workspaces = paginate_api_get(copy_url, copy_headers, "workspaces")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                task_logger.warning(
                    "403 Forbidden: COPY_API_TOKEN lacks permission to list workspaces. "
                    "Ensure the token has ORGANIZATION_OWNER role. "
                    "Skipping workspace-level sync."
                )
            else:
                raise

        master_by_name = {ws["name"]: ws for ws in master_workspaces}
        copy_by_name = {ws["name"]: ws for ws in copy_workspaces}

        common_names = set(master_by_name.keys()) & set(copy_by_name.keys())
        master_only = set(master_by_name.keys()) - set(copy_by_name.keys())

        task_logger.info(
            f"Found {len(master_workspaces)} master workspaces, "
            f"{len(copy_workspaces)} copy workspaces, "
            f"{len(common_names)} in common"
        )
        if master_only:
            task_logger.info(
                f"[WS] Skipping {len(master_only)} workspace(s) only in Master: "
                f"{', '.join(sorted(master_only))}"
            )

        if not common_names:
            return {}

        master_ws_id_to_name = {ws["id"]: ws["name"] for ws in master_workspaces}
        copy_ws_id_to_name = {ws["id"]: ws["name"] for ws in copy_workspaces}

        # Fetch user profiles to discover workspace roles
        master_ws_members = {name: [] for name in common_names}
        for user in master_users:
            if not user.get("id"):
                continue
            url = (
                f"{ASTRO_API_BASE_URL}/v1/organizations/{master_org_id}"
                f"/users/{user['id']}"
            )
            try:
                user_data = make_api_request("GET", url, master_headers)
                for ws_role in user_data.get("workspaceRoles", []):
                    ws_id = ws_role.get("workspaceId")
                    ws_name = master_ws_id_to_name.get(ws_id)
                    if ws_name and ws_name in common_names:
                        master_ws_members[ws_name].append(
                            {
                                "email": user.get("email", ""),
                                "fullName": user.get("fullName", ""),
                                "id": user.get("id"),
                                "role": ws_role.get("role", "WORKSPACE_MEMBER"),
                            }
                        )
            except requests.exceptions.RequestException as e:
                task_logger.error(
                    f"Failed to get roles for master user {user.get('email')}: {e}"
                )

        # Fetch each copy user's full profile to discover workspace memberships
        copy_ws_members = {name: [] for name in common_names}  # ws_name -> [user]
        for user in copy_users:
            if not user.get("id"):
                continue
            url = (
                f"{ASTRO_API_BASE_URL}/v1/organizations/{copy_org_id}"
                f"/users/{user['id']}"
            )
            try:
                user_data = make_api_request("GET", url, copy_headers)
                for ws_role in user_data.get("workspaceRoles", []):
                    ws_id = ws_role.get("workspaceId")
                    ws_name = copy_ws_id_to_name.get(ws_id)
                    if ws_name and ws_name in common_names:
                        copy_ws_members[ws_name].append(
                            {
                                "email": user.get("email", ""),
                                "fullName": user.get("fullName", ""),
                                "id": user.get("id"),
                                "role": ws_role.get("role", "WORKSPACE_MEMBER"),
                            }
                        )
            except requests.exceptions.RequestException as e:
                task_logger.error(
                    f"Failed to get roles for copy user {user.get('email')}: {e}"
                )

        # Build workspace_data keyed by workspace name
        workspace_data = {}
        for ws_name in common_names:
            copy_ws_id = copy_by_name[ws_name]["id"]
            workspace_data[ws_name] = {
                "copy_ws_id": copy_ws_id,
                "master_users": master_ws_members[ws_name],
                "copy_users": copy_ws_members[ws_name],
            }
            task_logger.info(
                f"[WS:{ws_name}] {len(master_ws_members[ws_name])} master users, "
                f"{len(copy_ws_members[ws_name])} copy users"
            )

        return workspace_data

    @task
    def sync_workspace_users(
        config: dict,
        workspace_data: dict,
        copy_users: list[dict],
        teams_data: dict,
    ) -> dict:
        """Sync workspace-level user roles and team workspace roles."""
        import requests

        master_org_id = config["master_org_id"]
        copy_org_id = config["copy_org_id"]

        master_headers = {
            "Authorization": f"Bearer {MASTER_API_TOKEN}",
            "Content-Type": "application/json",
        }
        copy_headers = {
            "Authorization": f"Bearer {COPY_API_TOKEN}",
            "Content-Type": "application/json",
        }

        users_added = []
        users_removed = []
        roles_updated = []
        errors = []

        if not workspace_data and not teams_data:
            task_logger.info("[WS] No common workspaces or teams to sync")
            return {
                "users_added": users_added,
                "users_removed": users_removed,
                "roles_updated": roles_updated,
                "errors": errors,
            }

        # Build a lookup of copy org users by email for resolving user IDs
        copy_org_by_email = {
            u["email"].lower(): u for u in copy_users if u.get("email")
        }

        def get_current_roles(user_id: str) -> dict:
            """Fetch user's current roles to merge changes."""
            url = f"{ASTRO_API_BASE_URL}/v1/organizations/{copy_org_id}/users/{user_id}"
            return make_api_request("GET", url, copy_headers)

        def update_roles(user_id: str, org_role: str, ws_roles: list[dict]):
            """Update user's complete role set."""
            url = (
                f"{ASTRO_API_BASE_URL}/v1/organizations/{copy_org_id}"
                f"/users/{user_id}/roles"
            )
            payload = {
                "organizationRole": org_role,
                "workspaceRoles": ws_roles,
            }
            response = requests.post(url, headers=copy_headers, json=payload)
            response.raise_for_status()

        # Per-user workspace role sync
        if workspace_data:
            task_logger.info(f"Syncing {len(workspace_data)} workspace(s)")

            for ws_name, ws_info in workspace_data.items():
                copy_ws_id = ws_info["copy_ws_id"]
                master_by_email = {
                    u["email"].lower(): u
                    for u in ws_info["master_users"]
                    if u.get("email")
                }
                copy_by_email = {
                    u["email"].lower(): u
                    for u in ws_info["copy_users"]
                    if u.get("email")
                }

                # Add users
                for email, m_user in master_by_email.items():
                    if email not in copy_by_email:
                        org_user = copy_org_by_email.get(email)
                        if not org_user or not org_user.get("id"):
                            task_logger.info(
                                f"[WS:{ws_name}] SKIP ADD {email}: "
                                f"user not yet in copy org"
                            )
                            continue
                        try:
                            current = get_current_roles(org_user["id"])
                            org_role = current.get(
                                "organizationRole", "ORGANIZATION_MEMBER"
                            )
                            existing_ws_roles = current.get("workspaceRoles", [])
                            merged = [
                                r
                                for r in existing_ws_roles
                                if r.get("workspaceId") != copy_ws_id
                            ]
                            merged.append(
                                {"workspaceId": copy_ws_id, "role": m_user["role"]}
                            )
                            update_roles(org_user["id"], org_role, merged)
                            users_added.append(
                                {
                                    "email": email,
                                    "role": m_user["role"],
                                    "name": m_user.get("fullName", ""),
                                    "level": "WORKSPACE",
                                    "workspace": ws_name,
                                }
                            )
                            task_logger.info(
                                f"[WS:{ws_name}] ✓ ADD: {email} as {m_user['role']}"
                            )
                        except requests.exceptions.RequestException as e:
                            error_msg = f"[WS:{ws_name}] ✗ ADD FAILED for {email}: {e}"
                            task_logger.error(error_msg)
                            errors.append(error_msg)

                # Remove users
                for email, c_user in copy_by_email.items():
                    if email not in master_by_email:
                        org_user = copy_org_by_email.get(email)
                        if not org_user or not org_user.get("id"):
                            continue
                        try:
                            current = get_current_roles(org_user["id"])
                            org_role = current.get(
                                "organizationRole", "ORGANIZATION_MEMBER"
                            )
                            existing_ws_roles = current.get("workspaceRoles", [])
                            merged = [
                                r
                                for r in existing_ws_roles
                                if r.get("workspaceId") != copy_ws_id
                            ]
                            update_roles(org_user["id"], org_role, merged)
                            users_removed.append(
                                {
                                    "email": email,
                                    "name": c_user.get("fullName", ""),
                                    "level": "WORKSPACE",
                                    "workspace": ws_name,
                                }
                            )
                            task_logger.info(f"[WS:{ws_name}] ✓ REMOVE: {email}")
                        except requests.exceptions.RequestException as e:
                            error_msg = (
                                f"[WS:{ws_name}] ✗ REMOVE FAILED for {email}: {e}"
                            )
                            task_logger.error(error_msg)
                            errors.append(error_msg)

                # Update roles
                for email, m_user in master_by_email.items():
                    if email in copy_by_email:
                        c_user = copy_by_email[email]
                        if m_user["role"] != c_user["role"]:
                            org_user = copy_org_by_email.get(email)
                            if not org_user or not org_user.get("id"):
                                continue
                            try:
                                current = get_current_roles(org_user["id"])
                                org_role = current.get(
                                    "organizationRole", "ORGANIZATION_MEMBER"
                                )
                                existing_ws_roles = current.get("workspaceRoles", [])
                                merged = [
                                    r
                                    for r in existing_ws_roles
                                    if r.get("workspaceId") != copy_ws_id
                                ]
                                merged.append(
                                    {
                                        "workspaceId": copy_ws_id,
                                        "role": m_user["role"],
                                    }
                                )
                                task_logger.info(
                                    f"[WS:{ws_name}] ROLE CHANGE: {email} "
                                    f"{c_user['role']} -> {m_user['role']}..."
                                )
                                update_roles(org_user["id"], org_role, merged)
                                roles_updated.append(
                                    {
                                        "email": email,
                                        "old_role": c_user["role"],
                                        "new_role": m_user["role"],
                                        "name": m_user.get("fullName", ""),
                                        "level": "WORKSPACE",
                                        "workspace": ws_name,
                                    }
                                )
                                task_logger.info(
                                    f"[WS:{ws_name}] ✓ Role updated for {email}: "
                                    f"{c_user['role']} -> {m_user['role']}"
                                )
                            except requests.exceptions.RequestException as e:
                                error_msg = (
                                    f"[WS:{ws_name}] ✗ ROLE UPDATE FAILED for "
                                    f"{email} ({c_user['role']} -> "
                                    f"{m_user['role']}): {e}"
                                )
                                task_logger.error(error_msg)
                                errors.append(error_msg)

        # Per-team workspace role sync
        master_teams = teams_data.get("master_teams", [])
        copy_teams = teams_data.get("copy_teams", [])
        master_teams_by_name = {t["name"]: t for t in master_teams}
        copy_teams_by_name = {t["name"]: t for t in copy_teams}

        # Remap workspace IDs by name
        master_ws_url = (
            f"{ASTRO_API_BASE_URL}/v1/organizations/{master_org_id}/workspaces"
        )
        copy_ws_url = f"{ASTRO_API_BASE_URL}/v1/organizations/{copy_org_id}/workspaces"
        try:
            master_wss = paginate_api_get(master_ws_url, master_headers, "workspaces")
        except requests.exceptions.RequestException:
            master_wss = []
        try:
            copy_wss = paginate_api_get(copy_ws_url, copy_headers, "workspaces")
        except requests.exceptions.RequestException:
            copy_wss = []

        master_ws_id_to_name = {ws["id"]: ws["name"] for ws in master_wss}
        copy_ws_name_to_id = {ws["name"]: ws["id"] for ws in copy_wss}

        def remap_team_ws_roles(master_ws_roles: list[dict]) -> list[dict]:
            """Translate master workspace IDs to copy workspace IDs by name."""
            remapped = []
            for wr in master_ws_roles:
                ws_name = master_ws_id_to_name.get(wr.get("workspaceId"))
                if ws_name and ws_name in copy_ws_name_to_id:
                    remapped.append(
                        {
                            "workspaceId": copy_ws_name_to_id[ws_name],
                            "role": wr.get("role", "WORKSPACE_MEMBER"),
                        }
                    )
            return remapped

        common_team_names = set(master_teams_by_name.keys()) & set(
            copy_teams_by_name.keys()
        )
        if common_team_names:
            task_logger.info(
                f"Syncing workspace roles for {len(common_team_names)} team(s)"
            )

        for team_name in common_team_names:
            m_team = master_teams_by_name[team_name]
            c_team = copy_teams_by_name[team_name]
            copy_team_id = c_team["id"]

            # Remap master team workspace roles to copy org workspace IDs
            desired_ws_roles = remap_team_ws_roles(m_team.get("workspaceRoles", []))
            current_ws_roles = c_team.get("workspaceRoles", [])

            # Normalise for comparison
            desired_set = {(r["workspaceId"], r["role"]) for r in desired_ws_roles}
            current_set = {
                (r.get("workspaceId"), r.get("role")) for r in current_ws_roles
            }

            if desired_set != current_set:
                try:
                    url = (
                        f"{ASTRO_API_BASE_URL}/v1/organizations/{copy_org_id}"
                        f"/teams/{copy_team_id}/roles"
                    )
                    payload = {
                        "organizationRole": c_team.get(
                            "organizationRole", "ORGANIZATION_MEMBER"
                        ),
                        "workspaceRoles": desired_ws_roles,
                    }
                    make_api_request("POST", url, copy_headers, json_data=payload)

                    # Describe what changed for reporting
                    added_ws = desired_set - current_set
                    removed_ws = current_set - desired_set
                    for ws_id, role in added_ws:
                        ws_label = next(
                            (n for n, i in copy_ws_name_to_id.items() if i == ws_id),
                            ws_id,
                        )
                        roles_updated.append(
                            {
                                "email": f"[Team: {team_name}]",
                                "old_role": "none",
                                "new_role": role,
                                "name": team_name,
                                "level": "WORKSPACE",
                                "workspace": ws_label,
                            }
                        )
                    for ws_id, role in removed_ws:
                        ws_label = next(
                            (n for n, i in copy_ws_name_to_id.items() if i == ws_id),
                            ws_id,
                        )
                        roles_updated.append(
                            {
                                "email": f"[Team: {team_name}]",
                                "old_role": role,
                                "new_role": "removed",
                                "name": team_name,
                                "level": "WORKSPACE",
                                "workspace": ws_label,
                            }
                        )
                    task_logger.info(
                        f"[WS:TEAMS] ✓ Team '{team_name}' workspace roles updated"
                    )
                except requests.exceptions.RequestException as e:
                    error_msg = (
                        f"[WS:TEAMS] ✗ FAILED to update workspace roles for "
                        f"team '{team_name}': {e}"
                    )
                    task_logger.error(error_msg)
                    errors.append(error_msg)

        return {
            "users_added": users_added,
            "users_removed": users_removed,
            "roles_updated": roles_updated,
            "errors": errors,
        }

    # =========================================================================
    # TEAM LEVEL SYNC
    # =========================================================================

    @task
    def get_teams(config: dict) -> dict:
        """Fetch teams, their members, and workspace roles from both orgs.

        For each org:
        1. GET /v1/organizations/{orgId}/teams — list all teams
           (each team object includes organizationRole AND workspaceRoles)
        2. GET /v1/organizations/{orgId}/teams/{teamId}/members — list members

        Returns a dict with master_teams and copy_teams, each being a list of
        team dicts with id, name, description, organizationRole, workspaceRoles,
        isIdpManaged, and members (list of user dicts with id/email).
        """
        import requests

        master_org_id = config["master_org_id"]
        copy_org_id = config["copy_org_id"]

        master_headers = {
            "Authorization": f"Bearer {MASTER_API_TOKEN}",
            "Content-Type": "application/json",
        }
        copy_headers = {
            "Authorization": f"Bearer {COPY_API_TOKEN}",
            "Content-Type": "application/json",
        }

        def fetch_teams_with_members(org_id, headers, label):
            """Fetch all teams, their members, and workspace roles for an org."""
            # 1. List all teams (includes organizationRole and workspaceRoles)
            url = f"{ASTRO_API_BASE_URL}/v1/organizations/{org_id}/teams"
            try:
                teams = paginate_api_get(url, headers, "teams")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403:
                    task_logger.warning(
                        f"403 Forbidden: {label} token lacks permission to list teams."
                    )
                    return []
                raise

            # 2. Enrich each team with members
            enriched = []
            for team in teams:
                team_id = team.get("id")

                # Fetch team members
                members_url = (
                    f"{ASTRO_API_BASE_URL}/v1/organizations/{org_id}"
                    f"/teams/{team_id}/members"
                )
                try:
                    members = paginate_api_get(members_url, headers, "teamMembers")
                except requests.exceptions.RequestException as e:
                    task_logger.error(
                        f"[TEAM:{team.get('name')}] Failed to get members "
                        f"from {label}: {e}"
                    )
                    members = []

                enriched.append(
                    {
                        "id": team_id,
                        "name": team.get("name", ""),
                        "description": team.get("description", ""),
                        "organizationRole": team.get(
                            "organizationRole", "ORGANIZATION_MEMBER"
                        ),
                        "workspaceRoles": team.get("workspaceRoles", []),
                        "isIdpManaged": team.get("isIdpManaged", False),
                        "rolesKeys": team.get("rolesKeys", []),
                        "members": [
                            {
                                "id": m.get("userId", m.get("id")),
                                "email": m.get("username", m.get("email", "")),
                                "fullName": m.get("fullName", ""),
                            }
                            for m in members
                        ],
                    }
                )

            task_logger.info(f"Retrieved {len(enriched)} teams from {label} org")
            return enriched

        master_teams = fetch_teams_with_members(master_org_id, master_headers, "master")
        copy_teams = fetch_teams_with_members(copy_org_id, copy_headers, "copy")

        return {
            "master_teams": master_teams,
            "copy_teams": copy_teams,
        }

    @task
    def sync_teams(
        config: dict,
        teams_data: dict,
        copy_users: list[dict],
    ) -> dict:
        """Sync teams: create, update, delete, and manage members."""
        import requests

        master_org_id = config["master_org_id"]
        copy_org_id = config["copy_org_id"]

        master_headers = {
            "Authorization": f"Bearer {MASTER_API_TOKEN}",
            "Content-Type": "application/json",
        }
        copy_headers = {
            "Authorization": f"Bearer {COPY_API_TOKEN}",
            "Content-Type": "application/json",
        }

        # Remap workspace IDs by name
        master_ws_url = (
            f"{ASTRO_API_BASE_URL}/v1/organizations/{master_org_id}/workspaces"
        )
        copy_ws_url = f"{ASTRO_API_BASE_URL}/v1/organizations/{copy_org_id}/workspaces"
        try:
            master_workspaces = paginate_api_get(
                master_ws_url, master_headers, "workspaces"
            )
        except requests.exceptions.RequestException:
            master_workspaces = []
        try:
            copy_workspaces = paginate_api_get(copy_ws_url, copy_headers, "workspaces")
        except requests.exceptions.RequestException:
            copy_workspaces = []

        # master ws ID -> ws name, and ws name -> copy ws ID
        master_ws_id_to_name = {ws["id"]: ws["name"] for ws in master_workspaces}
        copy_ws_name_to_id = {ws["name"]: ws["id"] for ws in copy_workspaces}

        def remap_workspace_roles(master_ws_roles: list[dict]) -> list[dict]:
            """Map master workspace IDs to copy workspace IDs by name."""
            remapped = []
            for wr in master_ws_roles:
                ws_name = master_ws_id_to_name.get(wr.get("workspaceId"))
                if ws_name and ws_name in copy_ws_name_to_id:
                    remapped.append(
                        {
                            "workspaceId": copy_ws_name_to_id[ws_name],
                            "role": wr.get("role", "WORKSPACE_MEMBER"),
                        }
                    )
                elif ws_name:
                    task_logger.info(
                        f"[TEAM] Skipping workspace role for '{ws_name}' "
                        f"(not in copy org)"
                    )
            return remapped

        master_teams = teams_data.get("master_teams", [])
        copy_teams = teams_data.get("copy_teams", [])

        # Build lookups by name
        master_by_name = {t["name"]: t for t in master_teams}
        copy_by_name = {t["name"]: t for t in copy_teams}

        copy_users_by_email = {
            u["email"].lower(): u for u in copy_users if u.get("email")
        }

        teams_created = []
        teams_deleted = []
        teams_updated = []
        members_added = []
        members_removed = []
        errors = []

        # Create new teams
        for name, m_team in master_by_name.items():
            if name not in copy_by_name:
                try:
                    url = f"{ASTRO_API_BASE_URL}/v1/organizations/{copy_org_id}/teams"
                    payload = {
                        "name": m_team["name"],
                        "description": m_team.get("description", ""),
                        "organizationRole": m_team["organizationRole"],
                    }
                    if m_team.get("rolesKeys"):
                        payload["rolesKeys"] = m_team["rolesKeys"]

                    result = make_api_request(
                        "POST", url, copy_headers, json_data=payload
                    )
                    new_team_id = result.get("id")
                    teams_created.append(
                        {
                            "name": name,
                            "role": m_team["organizationRole"],
                            "level": "TEAM",
                        }
                    )
                    task_logger.info(
                        f"[TEAM] ✓ CREATE: {name} (role: {m_team['organizationRole']})"
                    )

                    copy_by_name[name] = {
                        **m_team,
                        "id": new_team_id,
                        "members": [],
                    }
                except requests.exceptions.RequestException as e:
                    error_msg = f"[TEAM] ✗ CREATE FAILED for {name}: {e}"
                    task_logger.error(error_msg)
                    errors.append(error_msg)

        # Delete old teams
        for name, c_team in copy_by_name.items():
            if name not in master_by_name:
                try:
                    url = (
                        f"{ASTRO_API_BASE_URL}/v1/organizations/{copy_org_id}"
                        f"/teams/{c_team['id']}"
                    )
                    response = requests.delete(url, headers=copy_headers)
                    if response.status_code == 404:
                        task_logger.info(f"[TEAM] ✓ DELETE: {name} already gone (404)")
                    else:
                        response.raise_for_status()
                        task_logger.info(f"[TEAM] ✓ DELETE: {name}")
                    teams_deleted.append({"name": name, "level": "TEAM"})
                except requests.exceptions.RequestException as e:
                    error_msg = f"[TEAM] ✗ DELETE FAILED for {name}: {e}"
                    task_logger.error(error_msg)
                    errors.append(error_msg)

        # Update and sync members for common teams
        common_names = set(master_by_name.keys()) & set(copy_by_name.keys())
        for name in common_names:
            m_team = master_by_name[name]
            c_team = copy_by_name[name]
            copy_team_id = c_team["id"]

            # Update team description
            desc_changed = m_team.get("description", "") != c_team.get(
                "description", ""
            )
            if desc_changed:
                try:
                    url = (
                        f"{ASTRO_API_BASE_URL}/v1/organizations/{copy_org_id}"
                        f"/teams/{copy_team_id}"
                    )
                    payload = {
                        "name": m_team["name"],
                        "description": m_team.get("description", ""),
                    }
                    make_api_request("POST", url, copy_headers, json_data=payload)
                    teams_updated.append(
                        {
                            "name": name,
                            "change": "description updated",
                            "level": "TEAM",
                        }
                    )
                    task_logger.info(f"[TEAM] ✓ UPDATE: {name} (description)")
                except requests.exceptions.RequestException as e:
                    error_msg = f"[TEAM] ✗ UPDATE description FAILED for {name}: {e}"
                    task_logger.error(error_msg)
                    errors.append(error_msg)

            # Update team roles if different
            desired_ws_roles = remap_workspace_roles(m_team.get("workspaceRoles", []))
            current_ws_roles = c_team.get("workspaceRoles", [])
            desired_ws_set = {(r["workspaceId"], r["role"]) for r in desired_ws_roles}
            current_ws_set = {
                (r.get("workspaceId"), r.get("role")) for r in current_ws_roles
            }

            role_changed = m_team["organizationRole"] != c_team["organizationRole"]
            ws_roles_changed = desired_ws_set != current_ws_set

            if role_changed or ws_roles_changed:
                try:
                    url = (
                        f"{ASTRO_API_BASE_URL}/v1/organizations/{copy_org_id}"
                        f"/teams/{copy_team_id}/roles"
                    )
                    payload = {
                        "organizationRole": m_team["organizationRole"],
                        "workspaceRoles": desired_ws_roles,
                    }

                    make_api_request("POST", url, copy_headers, json_data=payload)
                    changes = []
                    if role_changed:
                        changes.append(
                            f"org role: {c_team['organizationRole']} -> "
                            f"{m_team['organizationRole']}"
                        )
                    if ws_roles_changed:
                        changes.append("workspace roles updated")
                    teams_updated.append(
                        {
                            "name": name,
                            "change": "; ".join(changes),
                            "level": "TEAM",
                        }
                    )
                    task_logger.info(
                        f"[TEAM] ✓ UPDATE ROLES: {name} ({'; '.join(changes)})"
                    )
                except requests.exceptions.RequestException as e:
                    error_msg = f"[TEAM] ✗ UPDATE ROLES FAILED for {name}: {e}"
                    task_logger.error(error_msg)
                    errors.append(error_msg)

            # Sync team members
            m_member_emails = {
                m["email"].lower() for m in m_team["members"] if m.get("email")
            }
            c_member_emails = {
                m["email"].lower() for m in c_team["members"] if m.get("email")
            }

            # Add members
            for email in m_member_emails - c_member_emails:
                copy_user = copy_users_by_email.get(email)
                if not copy_user or not copy_user.get("id"):
                    task_logger.info(
                        f"[TEAM:{name}] SKIP ADD {email}: user not yet in copy org"
                    )
                    continue
                try:
                    url = (
                        f"{ASTRO_API_BASE_URL}/v1/organizations/{copy_org_id}"
                        f"/teams/{copy_team_id}/members"
                    )
                    payload = {"memberIds": [copy_user["id"]]}
                    make_api_request("POST", url, copy_headers, json_data=payload)
                    members_added.append(
                        {
                            "email": email,
                            "team": name,
                            "level": "TEAM",
                        }
                    )
                    task_logger.info(f"[TEAM:{name}] ✓ ADD MEMBER: {email}")
                except requests.exceptions.RequestException as e:
                    error_msg = f"[TEAM:{name}] ✗ ADD MEMBER FAILED for {email}: {e}"
                    task_logger.error(error_msg)
                    errors.append(error_msg)

            # Remove members
            c_members_by_email = {
                m["email"].lower(): m for m in c_team["members"] if m.get("email")
            }
            for email in c_member_emails - m_member_emails:
                c_member = c_members_by_email.get(email)
                if not c_member or not c_member.get("id"):
                    continue
                try:
                    url = (
                        f"{ASTRO_API_BASE_URL}/v1/organizations/{copy_org_id}"
                        f"/teams/{copy_team_id}/members/{c_member['id']}"
                    )
                    response = requests.delete(url, headers=copy_headers)
                    if response.status_code == 404:
                        task_logger.info(
                            f"[TEAM:{name}] ✓ REMOVE MEMBER: {email} already gone (404)"
                        )
                    else:
                        response.raise_for_status()
                        task_logger.info(f"[TEAM:{name}] ✓ REMOVE MEMBER: {email}")
                    members_removed.append(
                        {
                            "email": email,
                            "team": name,
                            "level": "TEAM",
                        }
                    )
                except requests.exceptions.RequestException as e:
                    error_msg = f"[TEAM:{name}] ✗ REMOVE MEMBER FAILED for {email}: {e}"
                    task_logger.error(error_msg)
                    errors.append(error_msg)

        return {
            "teams_created": teams_created,
            "teams_deleted": teams_deleted,
            "teams_updated": teams_updated,
            "members_added": members_added,
            "members_removed": members_removed,
            "errors": errors,
        }

    # =========================================================================
    # DEPLOYMENT LEVEL ROLE SYNC
    # =========================================================================

    @task
    def sync_deployment_roles(
        config: dict,
        master_users: list[dict],
        copy_users: list[dict],
    ) -> dict:
        """Sync deployment-level roles for users from master to copy org.

        For each user, reads their deploymentRoles from the master org,
        remaps deployment IDs to the copy org (matched by deployment name,
        only within workspaces that exist in both orgs), and updates the
        user's roles in the copy org.

        Custom role names are resolved by name between orgs since role IDs
        differ. Built-in roles (e.g. DEPLOYMENT_ADMIN) are used as-is.
        """
        import requests

        master_org_id = config["master_org_id"]
        copy_org_id = config["copy_org_id"]

        master_headers = {
            "Authorization": f"Bearer {MASTER_API_TOKEN}",
            "Content-Type": "application/json",
        }
        copy_headers = {
            "Authorization": f"Bearer {COPY_API_TOKEN}",
            "Content-Type": "application/json",
        }

        roles_added = []
        roles_removed = []
        roles_updated = []
        errors = []

        if not master_users:
            task_logger.info("[DEPLOY] No master users to sync deployment roles for")
            return {
                "roles_added": roles_added,
                "roles_removed": roles_removed,
                "roles_updated": roles_updated,
                "errors": errors,
            }

        # --- Build deployment mapping (master name → copy ID) ---
        # Only include deployments in workspaces that exist in both orgs

        # 1. Get workspaces from both orgs
        master_ws_url = (
            f"{ASTRO_API_BASE_URL}/v1/organizations/{master_org_id}/workspaces"
        )
        copy_ws_url = f"{ASTRO_API_BASE_URL}/v1/organizations/{copy_org_id}/workspaces"
        try:
            master_wss = paginate_api_get(master_ws_url, master_headers, "workspaces")
        except requests.exceptions.RequestException:
            master_wss = []
        try:
            copy_wss = paginate_api_get(copy_ws_url, copy_headers, "workspaces")
        except requests.exceptions.RequestException:
            copy_wss = []

        master_ws_by_name = {ws["name"]: ws for ws in master_wss}
        copy_ws_by_name = {ws["name"]: ws for ws in copy_wss}
        common_ws_names = set(master_ws_by_name.keys()) & set(copy_ws_by_name.keys())

        if not common_ws_names:
            task_logger.info(
                "[DEPLOY] No common workspaces found — skipping deployment role sync"
            )
            return {
                "roles_added": roles_added,
                "roles_removed": roles_removed,
                "roles_updated": roles_updated,
                "errors": errors,
            }

        # 2. Get deployments from both orgs
        master_dep_url = (
            f"{ASTRO_API_BASE_URL}/v1/organizations/{master_org_id}/deployments"
        )
        copy_dep_url = (
            f"{ASTRO_API_BASE_URL}/v1/organizations/{copy_org_id}/deployments"
        )
        try:
            master_deps = paginate_api_get(
                master_dep_url, master_headers, "deployments"
            )
        except requests.exceptions.RequestException as e:
            task_logger.error(f"[DEPLOY] Failed to list master deployments: {e}")
            master_deps = []
        try:
            copy_deps = paginate_api_get(copy_dep_url, copy_headers, "deployments")
        except requests.exceptions.RequestException as e:
            task_logger.error(f"[DEPLOY] Failed to list copy deployments: {e}")
            copy_deps = []

        # Map master workspace IDs to names
        master_ws_id_to_name = {ws["id"]: ws["name"] for ws in master_wss}

        # Filter deployments to only those in common workspaces
        # Build: master deployment ID → deployment name
        master_dep_id_to_name = {}
        for dep in master_deps:
            ws_id = dep.get("workspaceId")
            ws_name = master_ws_id_to_name.get(ws_id)
            if ws_name and ws_name in common_ws_names:
                master_dep_id_to_name[dep["id"]] = dep.get("name", "")

        # Build: deployment name → copy deployment ID (scoped to common ws)
        copy_dep_name_to_id = {}
        copy_ws_id_to_name = {ws["id"]: ws["name"] for ws in copy_wss}
        for dep in copy_deps:
            ws_id = dep.get("workspaceId")
            ws_name = copy_ws_id_to_name.get(ws_id)
            if ws_name and ws_name in common_ws_names:
                copy_dep_name_to_id[dep.get("name", "")] = dep["id"]

        copy_dep_id_to_name = {v: k for k, v in copy_dep_name_to_id.items()}

        task_logger.info(
            f"[DEPLOY] {len(master_dep_id_to_name)} master deployments, "
            f"{len(copy_dep_name_to_id)} copy deployments in common workspaces"
        )

        # --- Build custom role name mapping (master role name → copy role ID) ---
        # Custom deployment roles are referenced by role name (not the built-in
        # DEPLOYMENT_ADMIN). We need to map master role names to copy role IDs.
        def fetch_custom_role_map(org_id, headers):
            """Return {role_name: role_id} for custom DEPLOYMENT-scoped roles."""
            url = f"{ASTRO_API_BASE_URL}/v1/organizations/{org_id}/roles"
            role_map = {}
            offset = 0
            limit = 100
            while True:
                params = {
                    "offset": offset,
                    "limit": limit,
                    "scopeTypes": "DEPLOYMENT",
                }
                try:
                    resp = requests.get(url, headers=headers, params=params)
                    resp.raise_for_status()
                    data = resp.json()
                except requests.exceptions.RequestException:
                    break
                for r in data.get("roles", []):
                    role_map[r["name"]] = r["id"]
                if len(data.get("roles", [])) < limit:
                    break
                offset += limit
            return role_map

        master_role_id_to_name = {}
        master_custom_roles = fetch_custom_role_map(master_org_id, master_headers)
        for name, rid in master_custom_roles.items():
            master_role_id_to_name[rid] = name

        copy_custom_roles = fetch_custom_role_map(copy_org_id, copy_headers)
        # copy_role_name_to_id = copy_custom_roles (name → id)

        def remap_role(master_role_value):
            """Translate a master role value to the copy org equivalent.

            Built-in roles like DEPLOYMENT_ADMIN pass through as-is.
            Custom role IDs are resolved by name to the copy org's ID.
            """
            # If it's a known custom role ID, look up by name
            role_name = master_role_id_to_name.get(master_role_value)
            if role_name:
                copy_id = copy_custom_roles.get(role_name)
                if copy_id:
                    return copy_id
                task_logger.warning(
                    f"[DEPLOY] Custom role '{role_name}' not found in "
                    f"copy org — skipping"
                )
                return None
            # Otherwise it's a built-in role string (e.g. DEPLOYMENT_ADMIN)
            return master_role_value

        # --- Per-user deployment role sync ---
        copy_users_by_email = {
            u["email"].lower(): u for u in copy_users if u.get("email")
        }

        for user in master_users:
            if not user.get("id") or not user.get("email"):
                continue
            email = user["email"].lower()

            # Get master user profile (includes deploymentRoles)
            master_profile_url = (
                f"{ASTRO_API_BASE_URL}/v1/organizations/{master_org_id}"
                f"/users/{user['id']}"
            )
            try:
                master_profile = make_api_request(
                    "GET", master_profile_url, master_headers
                )
            except requests.exceptions.RequestException as e:
                error_msg = f"[DEPLOY] Failed to get master profile for {email}: {e}"
                task_logger.error(error_msg)
                errors.append(error_msg)
                continue

            master_dep_roles = master_profile.get("deploymentRoles", [])

            # Remap master deployment roles to copy org
            desired_dep_roles = []
            for dr in master_dep_roles:
                m_dep_id = dr.get("deploymentId")
                dep_name = master_dep_id_to_name.get(m_dep_id)
                if not dep_name:
                    # Deployment not in a common workspace — skip
                    continue
                copy_dep_id = copy_dep_name_to_id.get(dep_name)
                if not copy_dep_id:
                    task_logger.info(
                        f"[DEPLOY] Skipping deployment '{dep_name}' for "
                        f"{email} (not in copy org)"
                    )
                    continue
                remapped_role = remap_role(dr.get("role", "DEPLOYMENT_ADMIN"))
                if not remapped_role:
                    continue
                desired_dep_roles.append(
                    {"deploymentId": copy_dep_id, "role": remapped_role}
                )

            # Get copy user profile
            copy_user = copy_users_by_email.get(email)
            if not copy_user or not copy_user.get("id"):
                if desired_dep_roles:
                    task_logger.info(f"[DEPLOY] SKIP {email}: not yet in copy org")
                continue

            copy_profile_url = (
                f"{ASTRO_API_BASE_URL}/v1/organizations/{copy_org_id}"
                f"/users/{copy_user['id']}"
            )
            try:
                copy_profile = make_api_request("GET", copy_profile_url, copy_headers)
            except requests.exceptions.RequestException as e:
                error_msg = f"[DEPLOY] Failed to get copy profile for {email}: {e}"
                task_logger.error(error_msg)
                errors.append(error_msg)
                continue

            current_dep_roles = copy_profile.get("deploymentRoles", [])

            # Normalise for comparison (only deployments in common workspaces)
            desired_set = {(r["deploymentId"], r["role"]) for r in desired_dep_roles}
            current_set = {
                (r.get("deploymentId"), r.get("role"))
                for r in current_dep_roles
                if r.get("deploymentId") in copy_dep_id_to_name
            }

            if desired_set == current_set:
                continue  # No changes needed

            # Merge: keep deployment roles for deployments NOT in scope,
            # replace those in scope with the desired set
            merged_dep_roles = [
                r
                for r in current_dep_roles
                if r.get("deploymentId") not in copy_dep_id_to_name
            ]
            merged_dep_roles.extend(desired_dep_roles)

            try:
                update_url = (
                    f"{ASTRO_API_BASE_URL}/v1/organizations/{copy_org_id}"
                    f"/users/{copy_user['id']}/roles"
                )
                payload = {
                    "organizationRole": copy_profile.get(
                        "organizationRole", "ORGANIZATION_MEMBER"
                    ),
                    "workspaceRoles": copy_profile.get("workspaceRoles", []),
                    "deploymentRoles": merged_dep_roles,
                }
                requests.post(
                    update_url, headers=copy_headers, json=payload
                ).raise_for_status()

                # Report what changed
                added = desired_set - current_set
                removed = current_set - desired_set
                for dep_id, role in added:
                    dep_label = copy_dep_id_to_name.get(dep_id, dep_id)
                    roles_added.append(
                        {
                            "email": email,
                            "role": role,
                            "deployment": dep_label,
                            "level": "DEPLOYMENT",
                        }
                    )
                    task_logger.info(
                        f"[DEPLOY] ✓ ADD: {email} as {role} on '{dep_label}'"
                    )
                for dep_id, role in removed:
                    dep_label = copy_dep_id_to_name.get(dep_id, dep_id)
                    roles_removed.append(
                        {
                            "email": email,
                            "role": role,
                            "deployment": dep_label,
                            "level": "DEPLOYMENT",
                        }
                    )
                    task_logger.info(
                        f"[DEPLOY] ✓ REMOVE: {email} role {role} from '{dep_label}'"
                    )

            except requests.exceptions.RequestException as e:
                error_msg = f"[DEPLOY] ✗ ROLE UPDATE FAILED for {email}: {e}"
                task_logger.error(error_msg)
                errors.append(error_msg)

        return {
            "roles_added": roles_added,
            "roles_removed": roles_removed,
            "roles_updated": roles_updated,
            "errors": errors,
        }

    # =========================================================================
    # REPORTING
    # =========================================================================

    @task
    def generate_sync_report(
        custom_roles_result: dict,
        org_result: dict,
        teams_result: dict,
        workspace_result: dict,
        deployment_result: dict,
    ) -> str:
        """Generate sync report with summary of all changes."""

        all_results = [
            org_result,
            teams_result,
            workspace_result,
            deployment_result,
        ]

        def count_all(key):
            return sum(len(r.get(key, [])) for r in all_results)

        total_invited = count_all("users_invited")
        total_added = count_all("users_added") + count_all("members_added")
        total_removed = count_all("users_removed") + count_all("members_removed")
        total_updated = count_all("roles_updated")
        total_teams_created = len(teams_result.get("teams_created", []))
        total_teams_deleted = len(teams_result.get("teams_deleted", []))
        total_teams_updated = len(teams_result.get("teams_updated", []))

        # Custom roles counts
        cr_created = custom_roles_result.get("roles_created", [])
        cr_updated = custom_roles_result.get("roles_updated", [])
        cr_deleted = custom_roles_result.get("roles_deleted", [])
        cr_errors = custom_roles_result.get("errors", [])
        total_cr_changes = len(cr_created) + len(cr_updated) + len(cr_deleted)

        # Deployment role counts
        dep_added = deployment_result.get("roles_added", [])
        dep_removed = deployment_result.get("roles_removed", [])
        dep_updated_list = deployment_result.get("roles_updated", [])
        dep_errors = deployment_result.get("errors", [])
        total_dep_changes = len(dep_added) + len(dep_removed) + len(dep_updated_list)

        total_errors = count_all("errors") + len(cr_errors)

        in_sync = (
            total_invited == 0
            and total_added == 0
            and total_removed == 0
            and total_updated == 0
            and total_teams_created == 0
            and total_teams_deleted == 0
            and total_teams_updated == 0
            and total_cr_changes == 0
            and total_dep_changes == 0
        )

        report_lines = [
            "=" * 70,
            "ASTRO ORGANIZATION SYNC REPORT",
            "=" * 70,
            "",
        ]

        if total_errors > 0:
            report_lines.append(f"⚠️  STATUS: COMPLETED WITH {total_errors} ERROR(S)")
            report_lines.append(
                "⚠️  Some operations failed — review errors below for details."
            )
            report_lines.append("")
        elif in_sync:
            report_lines.append("STATUS: Everything is in sync across all levels!")
            report_lines.append("")
        else:
            report_lines.append("STATUS: Changes were made during this sync")
            report_lines.append("")

        # --- Custom Roles details ---
        has_cr_changes = cr_created or cr_updated or cr_deleted or cr_errors
        if has_cr_changes:
            report_lines.append(f"{'─' * 70}")
            report_lines.append("CUSTOM ROLE CHANGES")
            report_lines.append(f"{'─' * 70}")

            if cr_created:
                report_lines.append(f"\n  ROLES CREATED ({len(cr_created)}):")
                for role in cr_created:
                    report_lines.append(f"    + {role['name']}")

            if cr_updated:
                report_lines.append(f"\n  ROLES UPDATED ({len(cr_updated)}):")
                for role in cr_updated:
                    report_lines.append(
                        f"    ~ {role['name']}: {role.get('change', 'updated')}"
                    )

            if cr_deleted:
                report_lines.append(f"\n  ROLES DELETED ({len(cr_deleted)}):")
                for role in cr_deleted:
                    report_lines.append(f"    - {role['name']}")

            if cr_errors:
                report_lines.append(
                    f"\n  ⚠️  ERRORS ({len(cr_errors)}) — ACTION REQUIRED:"
                )
                for error in cr_errors:
                    report_lines.append(f"    ✗ {error}")

            report_lines.append("")

        # --- Organization and Workspace level details ---
        for level_name, result in [
            ("ORGANIZATION", org_result),
            ("WORKSPACE", workspace_result),
        ]:
            level_invited = result.get("users_invited", [])
            level_added = result.get("users_added", [])
            level_removed = result.get("users_removed", [])
            level_updated = result.get("roles_updated", [])
            level_errors = result.get("errors", [])

            has_changes = (
                level_invited
                or level_added
                or level_removed
                or level_updated
                or level_errors
            )

            if has_changes:
                report_lines.append(f"{'─' * 70}")
                report_lines.append(f"{level_name} LEVEL CHANGES")
                report_lines.append(f"{'─' * 70}")

                if level_invited:
                    report_lines.append(
                        f"\n  USERS INVITED/ADDED ({len(level_invited)}):"
                    )
                    for user in level_invited:
                        context = ""
                        if user.get("workspace"):
                            context = f" [WS: {user['workspace']}]"
                        report_lines.append(
                            f"    * {user['email']} ({user.get('name', 'N/A')}) "
                            f"- Role: {user['role']}{context}"
                        )

                if level_added:
                    report_lines.append(f"\n  USERS ADDED ({len(level_added)}):")
                    for user in level_added:
                        context = ""
                        if user.get("workspace"):
                            context = f" [WS: {user['workspace']}]"
                        report_lines.append(
                            f"    + {user['email']} ({user.get('name', 'N/A')}) "
                            f"- Role: {user['role']}{context}"
                        )

                if level_removed:
                    report_lines.append(f"\n  USERS REMOVED ({len(level_removed)}):")
                    for user in level_removed:
                        context = ""
                        if user.get("workspace"):
                            context = f" [WS: {user['workspace']}]"
                        report_lines.append(
                            f"    - {user['email']} ({user.get('name', 'N/A')}){context}"
                        )

                if level_updated:
                    report_lines.append(f"\n  ROLES UPDATED ({len(level_updated)}):")
                    for user in level_updated:
                        context = ""
                        if user.get("workspace"):
                            context = f" [WS: {user['workspace']}]"
                        report_lines.append(
                            f"    ~ {user['email']} ({user.get('name', 'N/A')}): "
                            f"{user['old_role']} -> {user['new_role']}{context}"
                        )

                if level_errors:
                    report_lines.append(
                        f"\n  ⚠️  ERRORS ({len(level_errors)}) — ACTION REQUIRED:"
                    )
                    for error in level_errors:
                        report_lines.append(f"    ✗ {error}")

                report_lines.append("")

        # --- Teams level details ---
        t_created = teams_result.get("teams_created", [])
        t_deleted = teams_result.get("teams_deleted", [])
        t_updated = teams_result.get("teams_updated", [])
        t_members_added = teams_result.get("members_added", [])
        t_members_removed = teams_result.get("members_removed", [])
        t_errors = teams_result.get("errors", [])

        has_team_changes = (
            t_created
            or t_deleted
            or t_updated
            or t_members_added
            or t_members_removed
            or t_errors
        )

        if has_team_changes:
            report_lines.append(f"{'─' * 70}")
            report_lines.append("TEAM LEVEL CHANGES")
            report_lines.append(f"{'─' * 70}")

            if t_created:
                report_lines.append(f"\n  TEAMS CREATED ({len(t_created)}):")
                for team in t_created:
                    report_lines.append(
                        f"    + {team['name']} (role: {team.get('role', 'N/A')})"
                    )

            if t_deleted:
                report_lines.append(f"\n  TEAMS DELETED ({len(t_deleted)}):")
                for team in t_deleted:
                    report_lines.append(f"    - {team['name']}")

            if t_updated:
                report_lines.append(f"\n  TEAMS UPDATED ({len(t_updated)}):")
                for team in t_updated:
                    report_lines.append(
                        f"    ~ {team['name']}: {team.get('change', 'updated')}"
                    )

            if t_members_added:
                report_lines.append(f"\n  TEAM MEMBERS ADDED ({len(t_members_added)}):")
                for m in t_members_added:
                    report_lines.append(f"    + {m['email']} [Team: {m['team']}]")

            if t_members_removed:
                report_lines.append(
                    f"\n  TEAM MEMBERS REMOVED ({len(t_members_removed)}):"
                )
                for m in t_members_removed:
                    report_lines.append(f"    - {m['email']} [Team: {m['team']}]")

            if t_errors:
                report_lines.append(
                    f"\n  ⚠️  ERRORS ({len(t_errors)}) — ACTION REQUIRED:"
                )
                for error in t_errors:
                    report_lines.append(f"    ✗ {error}")

            report_lines.append("")

        # --- Deployment level details ---
        has_dep_changes = dep_added or dep_removed or dep_updated_list or dep_errors
        if has_dep_changes:
            report_lines.append(f"{'─' * 70}")
            report_lines.append("DEPLOYMENT LEVEL CHANGES")
            report_lines.append(f"{'─' * 70}")

            if dep_added:
                report_lines.append(f"\n  DEPLOYMENT ROLES ADDED ({len(dep_added)}):")
                for item in dep_added:
                    report_lines.append(
                        f"    + {item['email']} as {item['role']} "
                        f"[Deploy: {item.get('deployment', 'N/A')}]"
                    )

            if dep_removed:
                report_lines.append(
                    f"\n  DEPLOYMENT ROLES REMOVED ({len(dep_removed)}):"
                )
                for item in dep_removed:
                    report_lines.append(
                        f"    - {item['email']} role {item['role']} "
                        f"[Deploy: {item.get('deployment', 'N/A')}]"
                    )

            if dep_updated_list:
                report_lines.append(
                    f"\n  DEPLOYMENT ROLES UPDATED ({len(dep_updated_list)}):"
                )
                for item in dep_updated_list:
                    report_lines.append(
                        f"    ~ {item['email']}: {item.get('old_role', 'N/A')} "
                        f"-> {item.get('new_role', 'N/A')} "
                        f"[Deploy: {item.get('deployment', 'N/A')}]"
                    )

            if dep_errors:
                report_lines.append(
                    f"\n  ⚠️  ERRORS ({len(dep_errors)}) — ACTION REQUIRED:"
                )
                for error in dep_errors:
                    report_lines.append(f"    ✗ {error}")

            report_lines.append("")

        # Summary
        report_lines.extend(
            [
                "=" * 70,
                "SUMMARY",
                "=" * 70,
                "",
                "Level             | Invited | Added | Removed | Updated | Errors",
                "-" * 70,
                f"Custom Roles      | {'N/A':>7} | "
                f"{len(cr_created):5} | "
                f"{len(cr_deleted):7} | "
                f"{len(cr_updated):7} | "
                f"{len(cr_errors):6}",
                f"Organization      | {len(org_result.get('users_invited', [])):7} | "
                f"{len(org_result.get('users_added', [])):5} | "
                f"{len(org_result.get('users_removed', [])):7} | "
                f"{len(org_result.get('roles_updated', [])):7} | "
                f"{len(org_result.get('errors', [])):6}",
                f"Teams             | {'N/A':>7} | "
                f"{len(t_members_added):5} | "
                f"{len(t_members_removed):7} | "
                f"{total_teams_created + total_teams_deleted + total_teams_updated:7} | "
                f"{len(t_errors):6}",
                f"Workspace         | {len(workspace_result.get('users_invited', [])):7} | "
                f"{len(workspace_result.get('users_added', [])):5} | "
                f"{len(workspace_result.get('users_removed', [])):7} | "
                f"{len(workspace_result.get('roles_updated', [])):7} | "
                f"{len(workspace_result.get('errors', [])):6}",
                f"Deployment        | {'N/A':>7} | "
                f"{len(dep_added):5} | "
                f"{len(dep_removed):7} | "
                f"{len(dep_updated_list):7} | "
                f"{len(dep_errors):6}",
                "-" * 70,
                f"TOTAL             | {total_invited:7} | "
                f"{total_added + len(cr_created) + len(dep_added):5} | "
                f"{total_removed + len(cr_deleted) + len(dep_removed):7} | "
                f"{total_updated + total_teams_created + total_teams_deleted + total_teams_updated + len(cr_updated) + len(dep_updated_list):7} | "
                f"{total_errors:6}",
                "",
            ]
        )

        if total_errors > 0:
            overall = f"⚠️  COMPLETED WITH {total_errors} ERROR(S)"
        elif in_sync:
            overall = "IN SYNC"
        else:
            overall = "CHANGES MADE"

        report_lines.extend(
            [
                f"Overall Status: {overall}",
                "=" * 70,
            ]
        )

        report = "\n".join(report_lines)
        task_logger.info(f"\n{report}")

        return report

    # =========================================================================
    # DAG FLOW
    # =========================================================================

    # Step 1: Validate configuration
    config = validate_config()

    # Step 2: Sync Custom Roles (before any deployment role assignments)
    # Ensures custom deployment roles exist in the copy org so that
    # deployment-level role references are valid.
    custom_roles_result = sync_custom_roles(config)

    # Step 3: Sync Organization-level users (invite/remove/update roles)
    master_org_users = get_master_org_users(config)
    copy_org_users = get_copy_org_users(config)
    org_sync_result = sync_org_users(config, master_org_users, copy_org_users)
    custom_roles_result >> org_sync_result

    # Step 4: Sync Teams (after org sync so users exist in copy org)
    # Teams may grant workspace roles via inheritance, so they must be
    # synced before workspace-level user sync.
    teams_data = get_teams(config)
    teams_sync_result = sync_teams(config, teams_data, copy_org_users)
    org_sync_result >> teams_sync_result

    # Step 5: Sync Workspace-level users AND team workspace roles (after teams)
    # get_workspaces derives per-workspace user lists from each user's
    # profile via GET .../users/{userId} (which includes workspaceRoles).
    # sync_workspace_users handles both per-user AND per-team workspace roles.
    workspace_data = get_workspaces(config, master_org_users, copy_org_users)
    workspace_sync_result = sync_workspace_users(
        config, workspace_data, copy_org_users, teams_data
    )
    teams_sync_result >> workspace_sync_result

    # Step 6: Sync Deployment-level roles (after workspace sync)
    # Reads each user's deploymentRoles from master, remaps deployment
    # and custom role IDs by name, and updates the copy org. Only
    # processes deployments in workspaces that exist in both orgs.
    deployment_sync_result = sync_deployment_roles(
        config, master_org_users, copy_org_users
    )
    workspace_sync_result >> deployment_sync_result

    # Step 7: Generate comprehensive report
    generate_sync_report(
        custom_roles_result,
        org_sync_result,
        teams_sync_result,
        workspace_sync_result,
        deployment_sync_result,
    )


sync_astro_org_users()
