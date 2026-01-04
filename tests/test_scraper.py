"""
Test suite for TopDownHockey NHL Scraper.
Tests scraping functionality against a known game.
"""
import pytest
from TopDownHockey_Scraper.TopDownHockey_NHL_Scraper import full_scrape


# Test game: SJS vs TBL on 2026-01-03, final score 3-7
TEST_GAME_ID = 2025020649


class TestScraper:
    """Tests for the NHL scraper."""

    @pytest.fixture(scope="class")
    def scraped_data(self):
        """Scrape test game once for all tests in this class."""
        return full_scrape([TEST_GAME_ID], verbose=False)

    def test_scrape_returns_dataframe(self, scraped_data):
        """Test that scraping returns a non-empty DataFrame."""
        assert scraped_data is not None
        assert len(scraped_data) > 0

    def test_scrape_has_required_columns(self, scraped_data):
        """Test that scraped data has essential columns."""
        required_columns = [
            'game_id', 'game_date', 'event_type', 'game_period',
            'game_seconds', 'home_team', 'away_team', 'home_score',
            'away_score', 'coords_x', 'coords_y'
        ]
        for col in required_columns:
            assert col in scraped_data.columns, f"Missing column: {col}"

    def test_correct_game_id(self, scraped_data):
        """Test that the scraped game has the correct game ID."""
        assert (scraped_data['game_id'] == TEST_GAME_ID).all()

    def test_correct_teams(self, scraped_data):
        """Test that the correct teams are present."""
        assert scraped_data['home_team'].iloc[0] == 'SJS'
        assert scraped_data['away_team'].iloc[0] == 'TBL'

    def test_has_goals(self, scraped_data):
        """Test that goals were scraped."""
        goals = scraped_data[scraped_data['event_type'] == 'GOAL']
        assert len(goals) == 10, f"Expected 10 goals, got {len(goals)}"

    def test_final_score(self, scraped_data):
        """Test that final score is correct (SJS 3 - 7 TBL)."""
        final_row = scraped_data.iloc[-1]
        assert int(final_row['home_score']) == 3, "Home score should be 3"
        assert int(final_row['away_score']) == 7, "Away score should be 7"

    def test_has_coordinates(self, scraped_data):
        """Test that some events have coordinates from API."""
        events_with_coords = scraped_data[scraped_data['coordinate_source'] == 'api']
        assert len(events_with_coords) > 100, "Should have many events with API coordinates"

    def test_reasonable_row_count(self, scraped_data):
        """Test that we have a reasonable number of events."""
        assert len(scraped_data) > 500, "Should have at least 500 rows"
        assert len(scraped_data) < 2000, "Should have fewer than 2000 rows"

    def test_event_types_present(self, scraped_data):
        """Test that key event types are present."""
        event_types = set(scraped_data['event_type'].unique())
        required_events = {'GOAL', 'SHOT', 'FAC', 'HIT', 'BLOCK', 'CHANGE'}
        for event in required_events:
            assert event in event_types, f"Missing event type: {event}"
