namespace Account.DAL.Tests

open NUnit.Framework
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open System.Linq
open Amazon.Runtime
open Account.DAL

[<TestFixture>]
type DynamoDbAccountDALTests() =

    let adc = AmazonDynamoDBConfig()
    do adc.ServiceURL <- "http://localhost:8000"
    let ddb = new AmazonDynamoDBClient(BasicAWSCredentials("xxx", "xxx"), adc)
    let cfg =
        { TableName = "test"
          PKColumnName = "Username"
          TimeoutMs = None }

    let deltbl () = 
        let tables = ddb.ListTablesAsync().Result
        if tables.TableNames |> Seq.contains "test" then do ddb.DeleteTableAsync(DeleteTableRequest("test")).Result |> ignore

    let getTestee () = DynamoDbAccountDAL(cfg, ddb) :> IAccountDAL

    let getacc () = 
        { Username = "mgjam"
          Email = "12131213@seznam.cz"
          PasswordHash = "ph"
          Tags = [ ("tag", "value") ; ("tag2", "value2") ] |> Map.ofSeq }

    [<SetUp>]
    member this.SetUp () = 
        do deltbl()
        do ddb.CreateTableAsync(CreateTableRequest(cfg.TableName, 
                                                   [ KeySchemaElement(cfg.PKColumnName, KeyType.HASH) ].ToList(), 
                                                   [ AttributeDefinition(cfg.PKColumnName, ScalarAttributeType.S) ].ToList(),
                                                   ProvisionedThroughput(5L, 5L))).Result |> ignore

    [<TearDown>]
    member this.TearDown () =
        do deltbl()

    [<Test>]
    member this.``Should Create Account Successfuly`` () =
        let account = getacc()
        let result = getTestee().CreateAccount account |> Async.RunSynchronously
        match result with
        | CreateAccountResult.Account a -> Assert.AreEqual(account, a)
        | _ -> Assert.Fail()

    [<Test>]
    member this.``Should Not Create Account When Username Already Exists`` () =
        let account = getacc()
        do getTestee().CreateAccount account |> Async.RunSynchronously |> ignore
        let result = getTestee().CreateAccount account |> Async.RunSynchronously
        match result with
        | CreateAccountResult.UsernameAlreadyExists -> Assert.Pass()
        | _ -> Assert.Fail()

    [<Test>]
    member this.``Should Verify Password Successfully`` () =
        let account = getacc()
        do getTestee().CreateAccount account |> Async.RunSynchronously |> ignore
        let result = getTestee().VerifyPassword account.Username account.PasswordHash |> Async.RunSynchronously
        match result with
        | PasswordResult.Account a -> Assert.AreEqual(account, a)
        | _ -> Assert.Fail()

    [<Test>]
    member this.``Should Not Verify Password When Invalid Username`` () =
        let account = getacc()
        do getTestee().CreateAccount account |> Async.RunSynchronously |> ignore
        let result = getTestee().VerifyPassword "wrong" account.PasswordHash |> Async.RunSynchronously
        match result with
        | PasswordResult.PasswordInvalid -> Assert.Pass()
        | _ -> Assert.Fail()

    [<Test>]
    member this.``Should Not Verify Password When Invalid Password`` () =
        let account = getacc()
        do getTestee().CreateAccount account |> Async.RunSynchronously |> ignore
        let result = getTestee().VerifyPassword account.Username "wrong" |> Async.RunSynchronously
        match result with
        | PasswordResult.PasswordInvalid -> Assert.Pass()
        | _ -> Assert.Fail()

    [<Test>]
    member this.``Should Change Password Successfully`` () =
        let account = getacc()
        let newPasswordHash = "ph2"
        do getTestee().CreateAccount account |> Async.RunSynchronously |> ignore
        let result = getTestee().ChangePassword account.Username account.PasswordHash newPasswordHash |> Async.RunSynchronously
        match result with
        | PasswordResult.Account a -> Assert.AreEqual(newPasswordHash, a.PasswordHash)
        | _ -> Assert.Fail()

    [<Test>]
    member this.``Should Not Change Password When Invalid Password`` () =
        let account = getacc()
        do getTestee().CreateAccount account |> Async.RunSynchronously |> ignore
        let result = getTestee().ChangePassword account.Username "wrong" "xxx" |> Async.RunSynchronously
        match result with
        | PasswordResult.PasswordInvalid -> Assert.Pass()
        | _ -> Assert.Fail()

    [<Test>]
    member this.``Should Not Change Password When Invalid Username`` () =
        let account = getacc()
        do getTestee().CreateAccount account |> Async.RunSynchronously |> ignore
        let result = getTestee().ChangePassword "wrong" account.PasswordHash "xxx" |> Async.RunSynchronously
        match result with
        | PasswordResult.PasswordInvalid -> Assert.Pass()
        | _ -> Assert.Fail()

    [<Test>]
    member this.``Should Reset Password Successfully`` () =
        let account = getacc()
        let newPasswordHash = "ph2"
        do getTestee().CreateAccount account |> Async.RunSynchronously |> ignore
        let result = getTestee().ResetPassword account.Username newPasswordHash |> Async.RunSynchronously
        match result with
        | UpdateResult.Account a -> Assert.AreEqual(newPasswordHash, a.PasswordHash)
        | _ -> Assert.Fail()

    [<Test>]
    member this.``Should Not Reset Password When Invalid Username`` () =
        let account = getacc()
        do getTestee().CreateAccount account |> Async.RunSynchronously |> ignore
        let result = getTestee().ResetPassword "wrong" "xxx" |> Async.RunSynchronously
        match result with
        | UpdateResult.AccountNotFound -> Assert.Pass()
        | _ -> Assert.Fail()

    [<Test>]
    member this.``Should Update Tags Successfully`` () =
        let account = getacc()
        let tags = [ ("tag", "value") ; ("tag3", "value3") ] |> Map.ofSeq
        do getTestee().CreateAccount account |> Async.RunSynchronously |> ignore
        let result = getTestee().UpdateTags account.Username tags |> Async.RunSynchronously
        match result with
        | UpdateResult.Account a -> Assert.AreEqual(tags, a.Tags)
        | _ -> Assert.Fail()

    [<Test>]
    member this.``Should Not Update Tags When Invalid Username`` () =
        let account = getacc()
        let tags = [ ("tag", "value") ; ("tag3", "value3") ] |> Map.ofSeq
        do getTestee().CreateAccount account |> Async.RunSynchronously |> ignore
        let result = getTestee().UpdateTags "wrong" tags |> Async.RunSynchronously
        match result with
        | UpdateResult.AccountNotFound -> Assert.Pass()
        | _ -> Assert.Fail()

    [<Test>]
    member this.``Should Create Account - Change Password - Verify Password`` () =
        let account = getacc()
        do getTestee().CreateAccount account |> Async.RunSynchronously |> ignore
        let newPasswordHash = "ph2"
        do getTestee().ChangePassword account.Username account.PasswordHash newPasswordHash |> Async.RunSynchronously |> ignore
        let result = getTestee().VerifyPassword account.Username newPasswordHash |> Async.RunSynchronously
        match result with
        | PasswordResult.Account a -> Assert.AreEqual(newPasswordHash, a.PasswordHash)
        | _ -> Assert.Fail()