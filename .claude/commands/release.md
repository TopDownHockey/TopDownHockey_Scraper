# Release Skill

Create a new GitHub release for TopDownHockey_Scraper.

## Instructions

When the user invokes `/release`, follow these steps:

1. **Read the current version** from `setup.py` and `setup.cfg` (look for the `version="X.Y.Z"` and `version = "X.Y.Z"` lines)

2. **Check git status** to ensure working directory is clean (no uncommitted changes)
   - If there ARE uncommitted changes, ask the user if they want to commit them first
   - If yes: stage and commit the changes with an appropriate message, then bump the version
   - Don't silently skip uncommitted changes - they're often the reason for the release!

3. **Run the test suite** to verify the scraper works:
   ```bash
   PYTHONPATH="src:$PYTHONPATH" python3 -m pytest tests/ -v --tb=short
   ```
   If tests fail, stop and report the failure.

4. **Check if tag already exists**:
   ```bash
   git tag -l "vX.Y.Z"
   ```
   If tag exists, ask user if they want to bump the version first.

5. **Create and push the git tag**:
   ```bash
   git tag -a vX.Y.Z -m "Release vX.Y.Z"
   git push origin vX.Y.Z
   ```

6. **Create the GitHub release** using `gh`:
   ```bash
   gh release create vX.Y.Z --title "vX.Y.Z" --generate-notes
   ```
   The `--generate-notes` flag auto-generates release notes from commits since the last release.

7. **Report success** with a link to the new release.

## Notes

- **GitHub Action auto-creates releases**: When you push a version bump commit, a GitHub Action auto-creates a tag and draft release. You may need to update the release notes with `gh release edit vX.Y.Z --notes "..."` instead of creating a new release.
- Authentication is handled by the user's local `gh` CLI credentials (stored in system keyring, never in the repo)
- No API tokens or secrets should ever be committed to this repository
- If `gh auth status` fails, instruct the user to run `gh auth login`
