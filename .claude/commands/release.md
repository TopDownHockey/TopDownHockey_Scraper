# Release Skill

Create a new GitHub release for TopDownHockey_Scraper.

## Instructions

When the user invokes `/release`, follow these steps **automatically without asking**:

1. **Read the current version** from `setup.py` and `setup.cfg` (look for the `version="X.Y.Z"` and `version = "X.Y.Z"` lines)

2. **Check git status** for uncommitted changes
   - If there ARE uncommitted changes: **automatically commit them** with an appropriate message based on what changed
   - Uncommitted changes are usually the reason for the release, so always include them

3. **Bump the version** automatically:
   - Increment the patch version (X.Y.Z → X.Y.Z+1) in both `setup.py` and `setup.cfg`
   - Commit the version bump along with any other changes

4. **Run the test suite** (tests run automatically via pre-push hook, but if not):
   ```bash
   PYTHONPATH="src:$PYTHONPATH" python3 -m pytest tests/ -v --tb=short
   ```
   If tests fail, stop and report the failure.

5. **Push changes and create tag**:
   ```bash
   gh auth setup-git  # Ensure git can authenticate
   git push origin main
   git tag -a vX.Y.Z -m "Release vX.Y.Z"
   git push origin vX.Y.Z
   ```

6. **Create the GitHub release** using `gh`:
   ```bash
   gh release create vX.Y.Z --title "vX.Y.Z" --generate-notes
   ```
   The `--generate-notes` flag auto-generates release notes from commits since the last release.

7. **Report success** with a link to the new release.

## Key Behavior

- **DO NOT ASK** for permission to commit changes or bump version - just do it
- **DO NOT ASK** if the user wants to proceed - the `/release` command is the signal to proceed
- If the tag already exists, bump the version first, then continue
- If `gh auth status` fails, instruct the user to run `gh auth login`

## Notes

- Authentication is handled by the user's local `gh` CLI credentials (stored in system keyring, never in the repo)
- No API tokens or secrets should ever be committed to this repository
