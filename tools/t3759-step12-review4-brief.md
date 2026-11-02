# Review brief, round 4: the round-3 fixes to t-3759 steps 1 and 2

Narrow review. Repo `/Users/rsaunderson/Source/mine/starh2-wt-3737`, branch
`carmack/t-3737`. The commits under review are 2496115 (step 1) and bccd37c
(step 2). Round 3 reviewed d332346/d01817f; its findings are in the round-3
brief's answers and were fixed here. Review ONLY what changed:
`git diff d332346 2496115` and `git diff d01817f bccd37c`, plus the code those
changes call. Read committed objects, not the working tree. The premises in
`tools/t3759-step12-review3-brief.md` still hold; read them first.

The changes:
- Queued WINDOW_UPDATE credit: a timestamp per entry
  (`window_unsent_since_ns`, `conn_unsent_since_ns`); the cap promotes only
  expired entries; a drain promotes all; a refusal that empties an entry clears
  its time.
- HTTP/1.1 exit join waits on `reaper_has_job` (set when the job is queued,
  cleared when the completion is taken), not on the owner byte. A test hook
  `test_reaper_hold_post` holds the reaper between its claim and its post.
- `firstBareEnd`: a bare END_STREAM goes out with no connection credit.
- `noteConnCredit` ends the connection-window wait when credit arrives.

Look for: a race or ordering defect in `reaper_has_job`; a path where it is
never cleared (so `serve` waits for ever) or cleared early; a timestamp left
set or cleared wrongly; a bare END_STREAM that can now go out before DATA it
must follow, or twice; a clock ended by credit that did not arrive; a test that
would pass with the fix removed; a test hook that changes behaviour outside
tests.

Label each finding **[LOGIC]** or **[PREMISE]** (name the fact); unlabelled
findings are discarded. Give file:line, a concrete scenario and quoted code.
Say "no finding" for each change you checked and found sound.
