# Nedbank DE Challenge — How to run the project


Build your image

```bash
docker build -t my-submission:test .
```

Run locally with scoring-equivalent constraints

```bash
docker run --rm \
  --network=none \
  --memory=2g --memory-swap=2g \
  --cpus=2 \
  --read-only \
  --tmpfs /tmp:rw,size=512m \
  -v /tmp/test-data:/data \
  my-submission:test

echo "Exit code: $?"
```

Your pipeline must exit with code 0 for the scoring system to read your output.

Verify your output

```bash
ls /tmp/test-data/output/bronze/
ls /tmp/test-data/output/silver/
ls /tmp/test-data/output/gold/
```


