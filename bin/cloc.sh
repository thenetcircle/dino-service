#!/bin/bash
cloc --fullpath --not-match-d "site|dinofw/admin/static|docs|dist|\.git|\.idea|__pycache__|dinofw\.egg-info" .
