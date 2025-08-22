#!/usr/bin/env python3
"""
Setup script for PDF generation using Playwright
"""

import subprocess
import sys
import os

def run_command(command, description):
    """Run a command and handle errors"""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed:")
        print(f"   Error: {e.stderr}")
        return False

def main():
    """Setup PDF generation with Playwright"""
    print("🚀 Setting up PDF generation with Playwright")
    print("=" * 50)
    
    # Check if playwright is installed
    try:
        import playwright
        print("✅ Playwright Python package is installed")
    except ImportError:
        print("❌ Playwright Python package not found")
        print("Installing playwright...")
        if not run_command("pip install playwright", "Installing Playwright Python package"):
            print("Failed to install Playwright. Please install manually:")
            print("pip install playwright")
            return False
    
    # Install Playwright browsers
    if not run_command("playwright install chromium", "Installing Playwright Chromium browser"):
        print("Failed to install Playwright browsers. Please install manually:")
        print("playwright install chromium")
        return False
    
    # Test PDF generation capability
    print("\n🧪 Testing PDF generation capability...")
    try:
        from playwright.async_api import async_playwright
        import tempfile
        import asyncio
        
        async def test_pdf():
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                await page.set_content("<html><body><h1>Test PDF Generation</h1><p>This is a test page.</p></body></html>")
                
                with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
                    await page.pdf(path=tmp_file.name)
                    print(f"✅ PDF test successful: {tmp_file.name}")
                    os.unlink(tmp_file.name)  # Clean up test file
                
                await browser.close()
        
        asyncio.run(test_pdf())
        print("🎉 PDF generation setup completed successfully!")
        print("\nYou can now use the MediaStackSearch class with PDF generation.")
        
    except Exception as e:
        print(f"❌ PDF generation test failed: {e}")
        print("Please check the installation and try again.")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)




