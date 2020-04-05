using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using NUnit.Framework;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Account.DAL.DynamoDb.InteropTests
{
    [TestFixture]
    public class DynamoDbAccountDALInteropTests
    {
        private AmazonDynamoDBConfig adc;
        private AmazonDynamoDBClient ddb;
        private DynamoDbAccountDALConfig cfg;

        [SetUp]
        public void SetUp()
        {
            adc = new AmazonDynamoDBConfig
            {
                ServiceURL = "http://localhost:8000"
            };
            ddb = new AmazonDynamoDBClient(new BasicAWSCredentials("xxx", "xxx"), adc);
            cfg = new DynamoDbAccountDALConfig("test", "Login", Microsoft.FSharp.Core.FSharpOption<int>.None);

            DeleteTableAsync().Wait();
            ddb.CreateTableAsync(new CreateTableRequest("test", new[]
            {
                new KeySchemaElement(cfg.PKColumnName, KeyType.HASH)
            }.ToList(), new []
            {
                new AttributeDefinition(cfg.PKColumnName, ScalarAttributeType.S)
            }.ToList(), new ProvisionedThroughput(5, 5)))
            .Wait();
        }

        [TearDown]
        public void TearDown()
        {
            DeleteTableAsync().Wait();
        }

        [Test]
        public async Task Should_Create_Account_Successfully()
        {
            var account = GetAccount();
            var testee = GetTestee();

            var result = await testee.CreateAccountAsync(account);

            Assert.IsTrue(result.IsAccount);
            var casted = result as CreateAccountResult.Account;

            Assert.AreEqual(account, casted.Item);
        }

        [Test]
        public async Task Should_Not_Create_Account_When_Account_Already_Exists()
        {
            var account = GetAccount();
            var testee = GetTestee();

            await testee.CreateAccountAsync(account);
            var result = await testee.CreateAccountAsync(account);

            Assert.IsTrue(result.IsLoginAlreadyExists);
        }

        [Test]
        public async Task Should_Verify_Password_Successfully()
        {
            var account = GetAccount();
            var testee = GetTestee();

            await testee.CreateAccountAsync(account);
            var result = await testee.VerifyPasswordAsync(account.Login, account.PasswordHash);

            Assert.IsTrue(result.IsAccount);
            Assert.AreEqual(account, (result as PasswordResult.Account).Item);
        }

        [Test]
        public async Task Should_Not_Verify_Password_When_Invalid_Login_Or_Password()
        {
            var account = GetAccount();
            var testee = GetTestee();

            await testee.CreateAccountAsync(account);

            var result = await testee.VerifyPasswordAsync(account.Login, "wrong");
            Assert.IsTrue(result.IsPasswordInvalid);

            result = await testee.VerifyPasswordAsync("wrong", account.PasswordHash);
            Assert.IsTrue(result.IsPasswordInvalid);
        }

        [Test]
        public async Task Should_Change_Password_Successfully()
        {
            var account = GetAccount();
            var testee = GetTestee();

            await testee.CreateAccountAsync(account);

            var result = await testee.ChangePasswordAsync(account.Login, account.PasswordHash, "newPH");
            Assert.IsTrue(result.IsAccount);
            Assert.AreEqual("newPH", (result as PasswordResult.Account).Item.PasswordHash);
        }

        [Test]
        public async Task Should_Not_Change_Password_When_Invalid_Login_Or_Password()
        {
            var account = GetAccount();
            var testee = GetTestee();

            await testee.CreateAccountAsync(account);

            var result = await testee.ChangePasswordAsync(account.Login, "wrong", "newPH");
            Assert.IsTrue(result.IsPasswordInvalid);

            result = await testee.ChangePasswordAsync("wrong", account.PasswordHash, "newPH");
            Assert.IsTrue(result.IsPasswordInvalid);
        }

        [Test]
        public async Task Should_Reset_Password_Successfully()
        {
            var account = GetAccount();
            var testee = GetTestee();

            await testee.CreateAccountAsync(account);
            var result = await testee.ResetPasswordAsync(account.Login, "newPH");

            Assert.IsTrue(result.IsAccount);
            Assert.AreEqual("newPH", (result as UpdateResult.Account).Item.PasswordHash);
        }

        [Test]
        public async Task Should_Not_Reset_Password_When_Account_Not_Found()
        {
            var account = GetAccount();
            var testee = GetTestee();

            await testee.CreateAccountAsync(account);
            var result = await testee.ResetPasswordAsync("wrong", "newPH");

            Assert.IsTrue(result.IsAccountNotFound);
        }

        [Test]
        public async Task Should_Update_Tags_Successfully()
        {
            var account = GetAccount();
            var testee = GetTestee();
            var newTags = new Microsoft.FSharp.Collections.FSharpMap<string, string>(new[]
            {
                Tuple.Create("tag3", "value3"), Tuple.Create("tag4", "value4")
            });
            await testee.CreateAccountAsync(account);
            var result = await testee.UpdateTagsAsync(account.Login, newTags);

            Assert.IsTrue(result.IsAccount);
            Assert.AreEqual(newTags, (result as UpdateResult.Account).Item.Tags);
        }

        [Test]
        public async Task Should_Not_Update_Tags_When_Account_Not_Found()
        {
            var account = GetAccount();
            var testee = GetTestee();
            var newTags = new Microsoft.FSharp.Collections.FSharpMap<string, string>(new[]
            {
                Tuple.Create("tag3", "value3"), Tuple.Create("tag4", "value4")
            });
            await testee.CreateAccountAsync(account);
            var result = await testee.UpdateTagsAsync("wrong", newTags);

            Assert.IsTrue(result.IsAccountNotFound);
        }

        [Test]
        public async Task Should_Create_Account_Change_Password_Verify_Password()
        {
            var account = GetAccount();
            var testee = GetTestee();

            await testee.CreateAccountAsync(account);
            var result = await testee.VerifyPasswordAsync(account.Login, account.PasswordHash);
            Assert.IsTrue(result.IsAccount);

            await testee.ChangePasswordAsync(account.Login, account.PasswordHash, "newPH");

            result = await testee.VerifyPasswordAsync(account.Login, "newPH");
            Assert.IsTrue(result.IsAccount);
        }

        private IAccountDAL GetTestee() => new DynamoDbAccountDAL(cfg, ddb);

        private async Task DeleteTableAsync()
        {
            var tables = await ddb.ListTablesAsync();

            if (tables.TableNames.Contains("test")) await ddb.DeleteTableAsync("test");
        }

        private Account GetAccount() => new Account("mgjam", "ph",
            new Microsoft.FSharp.Collections.FSharpMap<string, string>(new[]
            {
                Tuple.Create("tag", "value"), Tuple.Create("tag2", "value2")
            }));
    }
}
