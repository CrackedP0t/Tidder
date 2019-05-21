#!/bin/bash

pg_dump -O -s $@ > schema.sql
