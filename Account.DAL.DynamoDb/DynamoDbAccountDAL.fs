namespace Account.DAL

open Amazon.DynamoDBv2
open System.Threading
open Amazon.DynamoDBv2.Model
open System.Linq
open System
open System.Collections.Generic

type DynamoDbAccountDALConfig = 
    { TableName: string
      PKColumnName: string
      TimeoutMs: int option }

/// DynamoDb DAL for accounts
/// <exception cref="System.Threading.Tasks.TaskCanceledException">Thrown on timeout.</exception>
type DynamoDbAccountDAL
    ( cfg: DynamoDbAccountDALConfig,
      ddb: IAmazonDynamoDB ) =

    let emailColumnName = "Email"
    let passwordHashColumnName = "PasswordHash"
    let tagsColumnName = "Tags"

    let todct lst = Enumerable.ToDictionary(lst, fst, snd)

    let tomap (tags: seq<KeyValuePair<string, string>>) = 
        AttributeValue(M = (tags |> Seq.map (fun x -> (x.Key, AttributeValue(x.Value))) |> todct))

    let toacc (dct: Dictionary<string, AttributeValue>) = 
        { Username = dct.Item(cfg.PKColumnName).S
          Email = dct.Item(emailColumnName).S
          PasswordHash = dct.Item(passwordHashColumnName).S
          Tags = dct.Item(tagsColumnName).M |> Seq.map (fun x -> (x.Key, x.Value.S)) |> Map.ofSeq }

    let handleconex (ex: exn) handle =
        match ex with
        | :? AggregateException as agg -> match agg.InnerException with
                                          | :? ConditionalCheckFailedException -> handle
                                          | x -> raise(AggregateException(x))
        | x -> raise(AggregateException(x))

    let tknsrc () = new CancellationTokenSource(if cfg.TimeoutMs.IsNone then 10000 else cfg.TimeoutMs.Value)

    let updreq (username: string) = 
        let req = UpdateItemRequest()
        do req.TableName <- cfg.TableName
        do req.Key <- [ (cfg.PKColumnName, AttributeValue(username)) ] |> todct
        do req.ReturnValues <- ReturnValue.ALL_NEW
        req

    interface IAccountDAL with
        member this.CreateAccount account = async {
            try
                use src = tknsrc()
                let req = PutItemRequest(cfg.TableName, 
                                            [ (cfg.PKColumnName, AttributeValue(account.Username));
                                              (emailColumnName, AttributeValue(account.Email));
                                              (passwordHashColumnName, AttributeValue(account.PasswordHash));
                                              (tagsColumnName, account.Tags |> tomap)
                                            ] |> todct,
                                            ConditionExpression = sprintf "attribute_not_exists(%s)" cfg.PKColumnName)
                do! ddb.PutItemAsync(req, src.Token) |> Async.AwaitTask |> Async.Ignore
                return CreateAccountResult.Account(account)
            with
            | x -> return handleconex x CreateAccountResult.UsernameAlreadyExists
        }

        member this.VerifyPassword username passwordHash = async {
            let src = tknsrc()
            let req = QueryRequest(TableName = cfg.TableName, 
                                   KeyConditionExpression = sprintf "%s = :username" cfg.PKColumnName,
                                   FilterExpression = sprintf "%s = :hash" passwordHashColumnName,
                                   ExpressionAttributeValues = ([ (":username", AttributeValue(username));
                                                                  (":hash", AttributeValue(passwordHash))
                                                                ] |> todct))
            let! res = ddb.QueryAsync(req, src.Token) |> Async.AwaitTask
            match res.Count with
            | 0 -> return PasswordResult.PasswordInvalid
            | _ -> return PasswordResult.Account(res.Items.First() |> toacc)
        }

        member this.ChangePassword username passwordHash newPasswordHash = async {
            try
                let src = tknsrc()
                let req = updreq(username)
                do req.ConditionExpression <- sprintf "attribute_exists(%s) and %s = :hash" cfg.PKColumnName passwordHashColumnName
                do req.UpdateExpression <- sprintf "SET %s = :newHash" passwordHashColumnName
                do req.ExpressionAttributeValues <- [ (":hash", AttributeValue(passwordHash));
                                                      (":newHash", AttributeValue(newPasswordHash))
                                                    ] |> todct
                let! res = ddb.UpdateItemAsync(req, src.Token) |> Async.AwaitTask
                return PasswordResult.Account(res.Attributes |> toacc)
            with
            | x -> return handleconex x PasswordResult.PasswordInvalid
        }

        member this.ResetPassword username newPasswordHash = async {
            try
                let src = tknsrc()
                let req = updreq(username)
                do req.ConditionExpression <- sprintf "attribute_exists(%s)" cfg.PKColumnName
                do req.UpdateExpression <- sprintf "SET %s = :newHash" passwordHashColumnName
                do req.ExpressionAttributeValues <- [ (":newHash", AttributeValue(newPasswordHash)) ] |> todct
                let! res = ddb.UpdateItemAsync(req, src.Token) |> Async.AwaitTask
                return UpdateResult.Account(res.Attributes |> toacc)
            with
            | x -> return handleconex x UpdateResult.AccountNotFound
        }

        member this.UpdateTags username tags = async {
            try
                let src = tknsrc()
                let req = updreq(username)
                do req.ConditionExpression <- sprintf "attribute_exists(%s)" cfg.PKColumnName
                do req.UpdateExpression <- sprintf "SET %s = :t" tagsColumnName
                do req.ExpressionAttributeValues <- [ (":t", tags |> tomap) ] |> todct
                let! res = ddb.UpdateItemAsync(req, src.Token) |> Async.AwaitTask
                return UpdateResult.Account(res.Attributes |> toacc)
            with
            | x -> return handleconex x UpdateResult.AccountNotFound
        }