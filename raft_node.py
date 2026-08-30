import os
import time
import json
import random
import logging
import threading
import requests
from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import credentials, db

NODE_ID = int(os.getenv("NODE_ID", "1"))
DB_URL = os.getenv("DBURL")

# Raft timing configuration
HEARTBEAT_INTERVAL = 1.0
ELECTION_TIMEOUT_MIN = 3.0
ELECTION_TIMEOUT_MAX = 6.0
REQUEST_TIMEOUT = 1.5

app = Flask(__name__)

# Shared Raft state
STATE = "follower"
TERM = 0
VOTED_FOR = None
LEADER_ID = None
LAST_HEARTBEAT = time.time()
ELECTION_DEADLINE = 0

state_lock = threading.RLock()

# Peer addresses are discovered from the Docker Compose node IDs.
PEERS = {
    i: f"http://raft_node_{i}:5000"
    for i in range(1, 9)
    if i != NODE_ID
}

# Logging
os.makedirs("logs", exist_ok=True)
logger = logging.getLogger(f"raft-node-{NODE_ID}")
logger.setLevel(logging.INFO)
logger.propagate = False

if not logger.handlers:
    formatter = logging.Formatter(
        "%(asctime)s [Node %(node_id)s] %(levelname)s: %(message)s"
    )
    file_handler = logging.FileHandler(f"logs/node_{NODE_ID}.log")
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s [Node %(node_id)s] %(levelname)s: %(message)s"
    ))
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        "%(asctime)s [Node %(node_id)s] %(levelname)s: %(message)s"
    ))

    class NodeFilter(logging.Filter):
        def filter(self, record):
            record.node_id = NODE_ID
            return True

    file_handler.addFilter(NodeFilter())
    console_handler.addFilter(NodeFilter())
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


def reset_election_deadline():
    global ELECTION_DEADLINE
    ELECTION_DEADLINE = time.time() + random.uniform(
        ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX
    )


def initialize_firebase():
    if not DB_URL:
        logger.warning("DBURL is not configured; Firebase persistence disabled")
        return

    try:
        if not firebase_admin._apps:
            cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
            cred = credentials.Certificate(cred_path)
            firebase_admin.initialize_app(cred, {"databaseURL": DB_URL})
        logger.info("Firebase initialized")
    except Exception as exc:
        logger.error("Firebase initialization failed: %s", exc)


def persist_state():
    """Persist basic node state when Firebase is available."""
    try:
        if not firebase_admin._apps:
            return
        with state_lock:
            snapshot = {
                "node_id": NODE_ID,
                "term": TERM,
                "state": STATE,
                "voted_for": VOTED_FOR,
                "leader": LEADER_ID,
            }
        db.reference(f"nodes/{NODE_ID}").set(snapshot)
    except Exception as exc:
        logger.warning("Could not persist state: %s", exc)


def become_follower(term=None, leader_id=None):
    global STATE, TERM, VOTED_FOR, LEADER_ID, LAST_HEARTBEAT

    with state_lock:
        if term is not None and term > TERM:
            TERM = term
            VOTED_FOR = None
        STATE = "follower"
        if leader_id is not None:
            LEADER_ID = leader_id
        LAST_HEARTBEAT = time.time()
        reset_election_deadline()

    persist_state()


def request_vote(peer_id, term):
    try:
        response = requests.post(
            f"{PEERS[peer_id]}/vote",
            json={"term": term, "candidate_id": NODE_ID},
            timeout=REQUEST_TIMEOUT,
        )
        if response.ok:
            return response.json()
    except requests.RequestException as exc:
        logger.debug("Vote request to Node %s failed: %s", peer_id, exc)
    except ValueError:
        logger.warning("Node %s returned invalid vote response", peer_id)
    return None


def run_election():
    global STATE, TERM, VOTED_FOR, LEADER_ID

    with state_lock:
        if STATE == "leader":
            return
        STATE = "candidate"
        TERM += 1
        VOTED_FOR = NODE_ID
        LEADER_ID = None
        election_term = TERM
        reset_election_deadline()

    logger.info("Starting election for term %s", election_term)
    persist_state()

    votes = 1
    majority = (len(PEERS) + 1) // 2 + 1

    # Concurrent vote requests prevent one unavailable peer from blocking the
    # entire election cycle.
    results = []
    threads = []

    def collect_vote(peer_id):
        result = request_vote(peer_id, election_term)
        results.append(result)

    for peer_id in PEERS:
        thread = threading.Thread(target=collect_vote, args=(peer_id,), daemon=True)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join(timeout=REQUEST_TIMEOUT + 0.2)

    with state_lock:
        if STATE != "candidate" or TERM != election_term:
            return

        for result in results:
            if not result:
                continue
            response_term = result.get("term", election_term)
            if response_term > TERM:
                TERM = response_term
                STATE = "follower"
                VOTED_FOR = None
                reset_election_deadline()
                persist_state()
                return
            if result.get("vote_granted"):
                votes += 1

        if votes >= majority:
            STATE = "leader"
            LEADER_ID = NODE_ID
            logger.info("Elected leader for term %s with %s/%s votes", TERM, votes, len(PEERS) + 1)
        else:
            STATE = "follower"
            VOTED_FOR = None
            reset_election_deadline()
            logger.warning("Election failed: received %s/%s votes", votes, len(PEERS) + 1)

    persist_state()


def send_heartbeats():
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        with state_lock:
            if STATE != "leader":
                continue
            term = TERM

        for peer_id, peer_url in PEERS.items():
            try:
                response = requests.post(
                    f"{peer_url}/heartbeat",
                    json={"term": term, "leader_id": NODE_ID},
                    timeout=REQUEST_TIMEOUT,
                )
                if response.ok:
                    data = response.json()
                    remote_term = data.get("term", term)
                    if remote_term > term:
                        become_follower(remote_term)
                        logger.info("Stepped down after learning about higher term %s", remote_term)
                        break
            except requests.RequestException as exc:
                logger.debug("Heartbeat to Node %s failed: %s", peer_id, exc)
            except ValueError:
                logger.debug("Node %s returned invalid heartbeat response", peer_id)


def election_loop():
    while True:
        time.sleep(0.1)
        with state_lock:
            deadline = ELECTION_DEADLINE
            state = STATE

        if state != "leader" and time.time() >= deadline:
            run_election()


@app.route("/vote", methods=["POST"])
def vote():
    global TERM, VOTED_FOR, STATE, LEADER_ID

    data = request.get_json(silent=True) or {}
    candidate_term = int(data.get("term", 0))
    candidate_id = data.get("candidate_id")

    with state_lock:
        if candidate_term < TERM:
            return jsonify({"term": TERM, "vote_granted": False})

        if candidate_term > TERM:
            TERM = candidate_term
            STATE = "follower"
            VOTED_FOR = None
            LEADER_ID = None

        can_vote = VOTED_FOR is None or VOTED_FOR == candidate_id
        if can_vote:
            VOTED_FOR = candidate_id
            LAST_HEARTBEAT = time.time()
            reset_election_deadline()
            persist_state()
            logger.info("Granted vote to Node %s for term %s", candidate_id, TERM)
            return jsonify({"term": TERM, "vote_granted": True})

        return jsonify({"term": TERM, "vote_granted": False})


@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    data = request.get_json(silent=True) or {}
    incoming_term = int(data.get("term", 0))
    leader_id = data.get("leader_id")

    with state_lock:
        if incoming_term < TERM:
            return jsonify({"term": TERM, "success": False}), 409

        if incoming_term > TERM:
            TERM = incoming_term
            VOTED_FOR = None

        STATE = "follower"
        LEADER_ID = leader_id
        LAST_HEARTBEAT = time.time()
        reset_election_deadline()
        current_term = TERM

    persist_state()
    return jsonify({"term": current_term, "success": True})


@app.route("/leader_announcement", methods=["POST"])
def leader_announcement():
    data = request.get_json(silent=True) or {}
    incoming_term = int(data.get("term", 0))
    leader_id = data.get("leader_id")

    with state_lock:
        if incoming_term < TERM:
            return jsonify({"term": TERM, "success": False}), 409
        if incoming_term > TERM:
            TERM = incoming_term
            VOTED_FOR = None
        STATE = "follower"
        LEADER_ID = leader_id
        LAST_HEARTBEAT = time.time()
        reset_election_deadline()
        current_term = TERM

    persist_state()
    return jsonify({"term": current_term, "success": True})


@app.route("/status", methods=["GET"])
def status():
    with state_lock:
        return jsonify({
            "node_id": NODE_ID,
            "state": STATE,
            "term": TERM,
            "voted_for": VOTED_FOR,
            "leader": LEADER_ID,
        })


def startup():
    initialize_firebase()
    reset_election_deadline()

    threading.Thread(target=election_loop, daemon=True).start()
    threading.Thread(target=send_heartbeats, daemon=True).start()

    logger.info("Node %s started", NODE_ID)
    logger.info("Peers: %s", list(PEERS.values()))


if __name__ == "__main__":
    startup()
    app.run(host="0.0.0.0", port=5000, threaded=True)
