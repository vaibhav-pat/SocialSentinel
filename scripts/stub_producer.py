# Minimal Kafka producer to smoke-test the pipeline
import json, time, random
from datetime import datetime, timezone
from kafka import KafkaProducer

BOOTSTRAP = "kafka:9092"
TOPICS = {
    "reddit": "raw_posts_reddit",
    "twitter": "raw_posts_twitter",
    "forums": "raw_posts_forums"
}

p = KafkaProducer(bootstrap_servers=BOOTSTRAP, value_serializer=lambda v: json.dumps(v).encode("utf-8"))

samples = [
    {"text": "I feel anxious and can't sleep lately", "src":"twitter"},
    {"text": "My mood has been really low, I think I'm depressed", "src":"reddit"},
    {"text": "Stressed about exams and future", "src":"forums"},
]

while True:
    s = random.choice(samples)
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if s["src"]=="twitter":
        msg = {"tweet_id": f"t{int(time.time()*1000)}", "author_id": f"user_{random.randint(1,5)}", "text": s["text"], "created_at": now_iso, "lang": "en"}
        p.send(TOPICS["twitter"], msg)
    elif s["src"]=="reddit":
        msg = {"submission_id": f"r{int(time.time()*1000)}", "author_name": f"user_{random.randint(1,5)}", "title": s["text"], "selftext":"", "created_utc": time.time(), "subreddit":"mentalhealth"}
        p.send(TOPICS["reddit"], msg)
    else:
        msg = {"post_hash": f"f{int(time.time()*1000)}", "url": "https://example.com", "post_text": s["text"], "scraped_at": time.time(), "author_handle": f"user_{random.randint(1,5)}"}
        p.send(TOPICS["forums"], msg)
    p.flush()
    print("sent:", s)
    time.sleep(1)
