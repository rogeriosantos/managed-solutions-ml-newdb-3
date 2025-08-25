"""
Enhanced script to test database connection with comprehensive diagnostics.
Run this after setting up your Railway database credentials in .env
"""

import asyncio
import json
from app.config.database import (
    check_database_connection,
    get_database_info,
    connection_manager,
    close_database
)


async def test_basic_connection():
    """Test basic database connectivity."""
    print("🔍 Testing basic database connection...")
    
    is_connected = await check_database_connection()
    if is_connected:
        print("✅ Database connection successful!")
        return True
    else:
        print("❌ Database connection failed!")
        return False


async def test_database_info():
    """Test database information retrieval."""
    print("\n🔍 Getting database information...")
    
    db_info = await get_database_info()
    if db_info:
        print("✅ Database information retrieved successfully:")
        print(f"   - Version: {db_info.get('version', 'Unknown')}")
        print(f"   - Database: {db_info.get('database_name', 'Unknown')}")
        print(f"   - joblog_ob table exists: {db_info.get('joblog_ob_exists', False)}")
        
        if db_info.get('joblog_ob_exists'):
            count = db_info.get('joblog_ob_count', 0)
            print(f"   - joblog_ob records: {count:,}")
        
        return True
    else:
        print("❌ Failed to retrieve database information")
        return False


async def test_health_check():
    """Test comprehensive health check."""
    print("\n🔍 Running comprehensive health check...")
    
    health_info = await connection_manager.health_check()
    
    if health_info["status"] == "healthy":
        print("✅ Database health check passed!")
        
        # Display connection pool info
        pool_info = health_info.get("connection_pool", {})
        print("📊 Connection Pool Status:")
        print(f"   - Pool size: {pool_info.get('size', 'Unknown')}")
        print(f"   - Checked in: {pool_info.get('checked_in', 'Unknown')}")
        print(f"   - Checked out: {pool_info.get('checked_out', 'Unknown')}")
        print(f"   - Overflow: {pool_info.get('overflow', 'Unknown')}")
        print(f"   - Invalid: {pool_info.get('invalid', 'Unknown')}")
        
        return True
    else:
        print("❌ Database health check failed!")
        error = health_info.get("error", "Unknown error")
        print(f"   Error: {error}")
        return False


async def test_crud_operations():
    """Test basic CRUD operations."""
    print("\n🔍 Testing CRUD operations...")
    
    test_results = await connection_manager.test_crud_operations()
    
    print("📋 CRUD Test Results:")
    print(f"   - Session creation: {'✅' if test_results['create_session'] else '❌'}")
    print(f"   - Query execution: {'✅' if test_results['execute_query'] else '❌'}")
    print(f"   - Transaction handling: {'✅' if test_results['transaction'] else '❌'}")
    
    if test_results.get("error"):
        print(f"   - Error: {test_results['error']}")
        return False
    
    all_passed = all([
        test_results['create_session'],
        test_results['execute_query'],
        test_results['transaction']
    ])
    
    if all_passed:
        print("✅ All CRUD operations test passed!")
    else:
        print("❌ Some CRUD operations failed!")
    
    return all_passed


async def run_all_tests():
    """Run all database tests."""
    print("🚀 Starting comprehensive database connection tests...")
    print("=" * 60)
    
    tests = [
        ("Basic Connection", test_basic_connection),
        ("Database Info", test_database_info),
        ("Health Check", test_health_check),
        ("CRUD Operations", test_crud_operations)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            results[test_name] = await test_func()
        except Exception as e:
            print(f"❌ {test_name} test failed with exception: {e}")
            results[test_name] = False
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name:20} {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All database tests passed! Your connection is working perfectly.")
    else:
        print("\n💡 Troubleshooting tips:")
        print("   1. Verify Railway database credentials in .env file")
        print("   2. Check your internet connection")
        print("   3. Ensure Railway database service is running")
        print("   4. Verify database URL format: mysql+aiomysql://user:password@host:port/database")
        print("   5. Check firewall settings if connecting from restricted network")
    
    return passed == total


async def main():
    """Main test function."""
    try:
        success = await run_all_tests()
        return 0 if success else 1
    except KeyboardInterrupt:
        print("\n⚠️  Tests interrupted by user")
        return 1
    except Exception as e:
        print(f"\n💥 Unexpected error during testing: {e}")
        return 1
    finally:
        # Clean up database connections
        await close_database()


if __name__ == "__main__":
    print("Enhanced Railway Database Connection Tester")
    print("Make sure you have set the correct credentials in .env file")
    print()
    
    exit_code = asyncio.run(main())
    exit(exit_code)