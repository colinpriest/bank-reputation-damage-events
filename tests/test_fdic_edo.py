"""
Tests for FDIC ED&O connector.
"""

import pytest
import asyncio
from datetime import date, datetime
from unittest.mock import Mock, patch, AsyncMock
from selectolax.parser import HTMLParser

from ingestion.connectors.fdic_edo import FdicEdoConnector
from ingestion.normalizers.events_model import Event


class TestFdicEdoConnector:
    """Test cases for FDIC ED&O connector."""
    
    @pytest.fixture
    def connector(self):
        """Create a connector instance for testing."""
        return FdicEdoConnector()
    
    @pytest.fixture
    def sample_html(self):
        """Sample HTML content for testing."""
        return """
        <html>
            <body>
                <section>
                    <h2>Press Release Orders</h2>
                    <a href="/orders/2024-001">FDIC Issues Consent Order Against ABC Bank</a>
                    <a href="/orders/2024-002">Civil Money Penalty Against XYZ Bank</a>
                </section>
                <div class="slds-card">
                    <h3>Recent Order</h3>
                    <a href="/orders/2024-003">Enforcement Action Against DEF Bank</a>
                </div>
            </body>
        </html>
        """
    
    @pytest.fixture
    def sample_pdf_text(self):
        """Sample PDF text content for testing."""
        return """
        FEDERAL DEPOSIT INSURANCE CORPORATION
        WASHINGTON, D.C.
        
        In the Matter of
        ABC BANK, N.A.
        City, State
        
        CONSENT ORDER
        
        The Federal Deposit Insurance Corporation ("FDIC") has determined that ABC BANK, N.A.
        has engaged in unsafe or unsound banking practices and violations of law and/or regulations.
        
        IT IS HEREBY ORDERED that:
        
        1. Civil Money Penalty: ABC BANK, N.A. shall pay a civil money penalty of $1,000,000.
        
        2. Effective Date: This Order shall become effective on January 15, 2024.
        
        3. Docket Number: FDIC-24-001
        
        Dated: January 15, 2024
        """
    
    def test_connector_initialization(self, connector):
        """Test connector initialization."""
        assert connector.source_name == "fdic_edo"
        assert connector.base_url == "https://orders.fdic.gov"
        assert connector.search_url == "https://orders.fdic.gov/s/"
    
    def test_extract_order_number_from_url(self, connector):
        """Test order number extraction from URL."""
        url = "/orders/2024-001"
        title = "FDIC Issues Consent Order"
        
        order_number = connector._extract_order_number(title, url)
        assert order_number == "2024-001"
    
    def test_extract_order_number_from_title(self, connector):
        """Test order number extraction from title."""
        url = "/some/url"
        title = "Order No. 2024-002 Against Bank"
        
        order_number = connector._extract_order_number(title, url)
        assert order_number == "2024-002"
    
    def test_extract_order_from_link(self, connector):
        """Test order extraction from link element."""
        # Mock link element
        link = Mock()
        link.attributes = {"href": "/orders/2024-001"}
        link.text.return_value = "FDIC Issues Consent Order Against ABC Bank"
        
        item = connector._extract_order_from_link(link)
        
        assert item is not None
        assert item["id"] == "2024-001"
        assert item["title"] == "FDIC Issues Consent Order Against ABC Bank"
        assert item["url"] == "https://orders.fdic.gov/orders/2024-001"
        assert item["order_number"] == "2024-001"
    
    def test_extract_order_from_card(self, connector):
        """Test order extraction from card element."""
        # Mock card element
        card = Mock()
        
        # Mock title element
        title_elem = Mock()
        title_elem.text.return_value = "Recent Order"
        card.css_first.return_value = title_elem
        
        # Mock link element
        link = Mock()
        link.attributes = {"href": "/orders/2024-003"}
        card.css_first.side_effect = [title_elem, link]
        
        item = connector._extract_order_from_card(card)
        
        assert item is not None
        assert item["id"] == "2024-003"
        assert item["title"] == "Recent Order"
        assert item["url"] == "https://orders.fdic.gov/orders/2024-003"
    
    def test_parse_pdf_text(self, connector):
        """Test PDF text parsing."""
        parsed = connector._parse_pdf_text(self.sample_pdf_text)
        
        assert parsed["institution"] == "ABC BANK, N.A."
        assert parsed["penalty_amount"] == 1000000
        assert "civil money penalty of $1,000,000" in parsed["penalty_text"]
        assert parsed["docket_number"] == "FDIC-24-001"
        assert parsed["event_date"] == date(2024, 1, 15)
    
    def test_parse_html_content(self, connector):
        """Test HTML content parsing."""
        html_content = """
        <html>
            <body>
                <h1>ABC Bank Consent Order</h1>
                <div class="date">January 15, 2024</div>
            </body>
        </html>
        """
        
        parsed = connector._parse_html_content(html_content)
        
        assert parsed["institution"] == "ABC Bank Consent Order"
        assert parsed["event_date"] == date(2024, 1, 15)
    
    def test_generate_summary(self, connector):
        """Test summary generation."""
        parsed = {
            "institution": "ABC Bank",
            "order_type": "Consent Order",
            "penalty_amount": 1000000,
            "state": "NY"
        }
        
        summary = connector._generate_summary(parsed)
        
        assert "FDIC enforcement action against ABC Bank" in summary
        assert "(Consent Order)" in summary
        assert "with penalty of $1,000,000" in summary
        assert "in NY" in summary
    
    @pytest.mark.asyncio
    async def test_discover_items(self, connector, sample_html):
        """Test item discovery."""
        with patch.object(connector, '_make_request') as mock_request:
            # Mock response
            mock_response = Mock()
            mock_response.text = sample_html
            mock_request.return_value = mock_response
            
            items = await connector.discover_items(date(2024, 1, 1))
            
            assert len(items) == 3
            assert any(item["order_number"] == "2024-001" for item in items)
            assert any(item["order_number"] == "2024-002" for item in items)
            assert any(item["order_number"] == "2024-003" for item in items)
    
    @pytest.mark.asyncio
    async def test_fetch_item_detail(self, connector):
        """Test item detail fetching."""
        item = {
            "id": "2024-001",
            "url": "https://orders.fdic.gov/orders/2024-001"
        }
        
        with patch.object(connector, '_make_request') as mock_request:
            # Mock HTML response
            html_response = Mock()
            html_response.text = "<html><body><h1>ABC Bank Order</h1></body></html>"
            
            # Mock PDF response
            pdf_response = Mock()
            pdf_response.content = b"PDF content"
            
            mock_request.side_effect = [html_response, pdf_response]
            
            with patch('pdfminer.high_level.extract_text') as mock_pdf:
                mock_pdf.return_value = "PDF text content"
                
                result = await connector.fetch_item_detail(item)
                
                assert result["html_content"] == "<html><body><h1>ABC Bank Order</h1></body></html>"
                assert result["pdf_text"] == "PDF text content"
    
    def test_parse_item(self, connector):
        """Test item parsing."""
        item_data = {
            "order_number": "2024-001",
            "title": "FDIC Consent Order",
            "url": "https://orders.fdic.gov/orders/2024-001",
            "metadata": {
                "institution": "ABC Bank",
                "date": date(2024, 1, 15)
            },
            "pdf_text": self.sample_pdf_text
        }
        
        parsed = connector.parse_item(item_data)
        
        assert parsed["external_id"] == "2024-001"
        assert parsed["title"] == "FDIC Consent Order"
        assert parsed["institution"] == "ABC BANK, N.A."  # From PDF text
        assert parsed["event_date"] == date(2024, 1, 15)
        assert parsed["penalty_amount"] == 1000000
    
    def test_normalize_item(self, connector):
        """Test item normalization."""
        parsed_data = {
            "external_id": "2024-001",
            "title": "FDIC Consent Order Against ABC Bank",
            "url": "https://orders.fdic.gov/orders/2024-001",
            "institution": "ABC Bank",
            "order_type": "Consent Order",
            "event_date": date(2024, 1, 15),
            "penalty_amount": 1000000,
            "penalty_text": "civil money penalty of $1,000,000",
            "summary": "FDIC enforcement action against ABC Bank (Consent Order) with penalty of $1,000,000"
        }
        
        event = connector.normalize_item(parsed_data)
        
        assert isinstance(event, Event)
        assert event.event_id == "fdic-edo-2024-001-2024-01-15"
        assert event.title == "FDIC Consent Order Against ABC Bank"
        assert event.institutions == ["ABC Bank"]
        assert event.categories == ["regulatory_action", "fine"]
        assert event.event_date == date(2024, 1, 15)
        assert event.amounts["penalties_usd"] == 1000000
        assert event.reputational_damage.materiality_score == 3  # $1M penalty
        assert event.reputational_damage.drivers.regulator_involved == ["FDIC"]
        assert event.confidence == "medium"  # No PDF text in this test
    
    def test_materiality_scoring(self, connector):
        """Test materiality score calculation."""
        # Test $1M penalty (score 2)
        event_data = {
            "amounts": {"penalties_usd": 1000000},
            "title": "Test Order",
            "categories": ["regulatory_action"]
        }
        score = connector.calculate_materiality_score(event_data)
        assert score == 2
        
        # Test $100M penalty (score 4)
        event_data["amounts"]["penalties_usd"] = 100000000
        score = connector.calculate_materiality_score(event_data)
        assert score == 4
        
        # Test bank failure (score 5)
        event_data["categories"] = ["financial_performance"]
        score = connector.calculate_materiality_score(event_data)
        assert score == 5
    
    def test_category_mapping(self, connector):
        """Test category mapping."""
        # Test consent order
        category = connector.map_category("consent order")
        assert category == "regulatory_action"
        
        # Test civil money penalty
        category = connector.map_category("civil money penalty")
        assert category == "fine"
        
        # Test unknown category
        category = connector.map_category("unknown term")
        assert category == "other"
    
    def test_nature_mapping(self, connector):
        """Test nature mapping."""
        # Test compliance failure
        natures = connector.map_nature("compliance failure")
        assert "compliance_failure" in natures
        
        # Test multiple natures
        natures = connector.map_nature("bsa violation and customer trust")
        assert "sanctions_aml" in natures
        assert "customer_trust" in natures
        
        # Test unknown nature
        natures = connector.map_nature("unknown term")
        assert natures == ["other"]


class TestFdicEdoIntegration:
    """Integration tests for FDIC ED&O connector."""
    
    @pytest.mark.asyncio
    async def test_full_pipeline(self):
        """Test the complete pipeline from discovery to normalization."""
        connector = FdicEdoConnector()
        
        # Mock the entire pipeline
        with patch.object(connector, '_make_request') as mock_request:
            # Mock discovery response
            discovery_html = """
            <html>
                <body>
                    <section>
                        <h2>Press Release Orders</h2>
                        <a href="/orders/2024-001">FDIC Consent Order Against Test Bank</a>
                    </section>
                </body>
            </html>
            """
            
            # Mock detail response
            detail_html = """
            <html>
                <body>
                    <h1>Test Bank Consent Order</h1>
                    <div class="date">January 15, 2024</div>
                    <a href="/orders/2024-001.pdf">PDF</a>
                </body>
            </html>
            """
            
            # Mock PDF response
            pdf_content = """
            FEDERAL DEPOSIT INSURANCE CORPORATION
            
            In the Matter of
            TEST BANK, N.A.
            
            CONSENT ORDER
            
            Civil Money Penalty: $500,000
            
            Effective Date: January 15, 2024
            """
            
            mock_request.side_effect = [
                Mock(text=discovery_html),  # Discovery
                Mock(text=detail_html),     # Detail page
                Mock(content=pdf_content.encode())  # PDF
            ]
            
            with patch('pdfminer.high_level.extract_text') as mock_pdf:
                mock_pdf.return_value = pdf_content
                
                # Run the full pipeline
                events = await connector.fetch_updates(date(2024, 1, 1))
                
                assert len(events) == 1
                event = events[0]
                
                assert event.event_id == "fdic-edo-2024-001-2024-01-15"
                assert event.title == "FDIC Consent Order Against Test Bank"
                assert event.institutions == ["TEST BANK, N.A."]
                assert event.categories == ["regulatory_action", "fine"]
                assert event.amounts["penalties_usd"] == 500000
                assert event.reputational_damage.materiality_score == 2
                assert event.confidence == "high"


if __name__ == "__main__":
    pytest.main([__file__])



