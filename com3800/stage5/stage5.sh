#!/bin/bash
set -e

curl -d "public class Test{ public Test(){} public String run(){ return \"hello world!\";}}" \
  -H "Content-Type: text/x-java-source" -X POST http://localhost:8000/compileandrun
echo "test"