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
          PKColumnName = "Login"
          TimeoutMs = None }

    let deltbl () = 
        let tables = ddb.ListTablesAsync().Result
        if tables.TableNames |> Seq.contains "test" then do ddb.DeleteTableAsync(DeleteTableRequest("test")).Result |> ignore

    let getTestee () = DynamoDbAccountDAL(cfg, ddb) :> IAccountDAL

    let getacc () = 
        { Login = "mgjam"
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
    member this.``Should Not Create Account When Login Already Exists`` () =
        let account = getacc()
        do getTestee().CreateAccount account |> Async.RunSynchronously |> ignore
        let result = getTestee().CreateAccount account |> Async.RunSynchronously
        match result with
        | CreateAccountResult.LoginAlreadyExists -> Assert.Pass()
        | _ -> Assert.Fail()

    [<Test>]
    member this.``Should Verify Password Successfully`` () =
        let account = getacc()
        do getTestee().CreateAccount account |> Async.RunSynchronously |> ignore
        let result = getTestee().VerifyPassword account.Login account.PasswordHash |> Async.RunSynchronously
        match result with
        | PasswordResult.Account a -> Assert.AreEqual(account, a)
        | _ -> Assert.Fail()

    [<Test>]
    member this.``Should Not Verify Password When Invalid Login`` () =
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
        let result = getTestee().VerifyPassword account.Login "wrong" |> Async.RunSynchronously
        match result with
        | PasswordResult.PasswordInvalid -> Assert.Pass()
        | _ -> Assert.Fail()

    [<Test>]
    member this.``Should Change Password Successfully`` () =
        let account = getacc()
        let newPasswordHash = "ph2"
        do getTestee().CreateAccount account |> Async.RunSynchronously |> ignore
        let result = getTestee().ChangePassword account.Login account.PasswordHash newPasswordHash |> Async.RunSynchronously
        match result with
        | PasswordResult.Account a -> Assert.AreEqual(newPasswordHash, a.PasswordHash)
        | _ -> Assert.Fail()

    [<Test>]
    member this.``Should Not Change Password When Invalid Password`` () =
        let account = getacc()
        do getTestee().CreateAccount account |> Async.RunSynchronously |> ignore
        let result = getTestee().ChangePassword account.Login "wrong" "xxx" |> Async.RunSynchronously
        match result with
        | PasswordResult.PasswordInvalid -> Assert.Pass()
        | _ -> Assert.Fail()

    [<Test>]
    member this.``Should Not Change Password When Invalid Login`` () =
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
        let result = getTestee().ResetPassword account.Login newPasswordHash |> Async.RunSynchronously
        match result with
        | UpdateResult.Account a -> Assert.AreEqual(newPasswordHash, a.PasswordHash)
        | _ -> Assert.Fail()

    [<Test>]
    member this.``Should Not Reset Password When Invalid Login`` () =
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
        let result = getTestee().UpdateTags account.Login tags |> Async.RunSynchronously
        match result with
        | UpdateResult.Account a -> Assert.AreEqual(tags, a.Tags)
        | _ -> Assert.Fail()

    [<Test>]
    member this.``Should Not Update Tags When Invalid Login`` () =
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
        do getTestee().ChangePassword account.Login account.PasswordHash newPasswordHash |> Async.RunSynchronously |> ignore
        let result = getTestee().VerifyPassword account.Login newPasswordHash |> Async.RunSynchronously
        match result with
        | PasswordResult.Account a -> Assert.AreEqual(newPasswordHash, a.PasswordHash)
        | _ -> Assert.Fail()