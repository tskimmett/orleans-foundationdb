using TestExtensions;

namespace Orleans.FoundationDb.Tests;

[CollectionDefinition(TestEnvironmentFixture.DefaultCollection)]
public class TestEnvironmentFixtureCollection : ICollectionFixture<TestEnvironmentFixture>
{
}