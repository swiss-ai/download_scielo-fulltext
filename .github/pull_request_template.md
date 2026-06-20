## Summary

## Validation

- [ ] `bash scripts/check_repo.sh`
- [ ] RCP dry-run or smoke noted when operational behavior changes

## Safety

- [ ] No proxy credentials, generated corpus data, or local scratch paths committed
- [ ] Bulk-download changes preserve proxy-only egress and explicit all-shard gates
- [ ] License-filtering changes preserve CC BY / CC0 / public-domain-like only
