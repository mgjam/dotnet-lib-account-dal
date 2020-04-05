namespace Account.DAL

open System.Threading.Tasks

type Account = 
    { Login: string
      PasswordHash: string
      Tags: Map<string, string> }

type CreateAccountResult =
    | Account of Account
    | LoginAlreadyExists

type PasswordResult = 
    | Account of Account
    | PasswordInvalid

type UpdateResult = 
    | Account of Account
    | AccountNotFound

type IAccountDAL = 
    abstract member CreateAccount: 
        account: Account ->
        Async<CreateAccountResult>
    abstract member VerifyPassword:
        login: string -> 
        passwordHash: string -> 
        Async<PasswordResult>
    abstract member ChangePassword:
        login: string ->
        passwordHash: string ->
        newPasswordHash: string ->
        Async<PasswordResult>
    abstract member ResetPassword:
        login: string ->
        newPasswordHash: string ->
        Async<UpdateResult>
    abstract member UpdateTags:
        login: string ->
        tags: Map<string, string> ->
        Async<UpdateResult>

    abstract member CreateAccountAsync: 
        account: Account ->
        Task<CreateAccountResult>
    abstract member VerifyPasswordAsync:
        login: string * passwordHash: string ->
        Task<PasswordResult>
    abstract member ChangePasswordAsync:
        login: string * passwordHash: string * newPasswordHash: string ->
        Task<PasswordResult>
    abstract member ResetPasswordAsync:
        login: string * newPasswordHash: string ->
        Task<UpdateResult>
    abstract member UpdateTagsAsync:
        login: string * tags: Map<string, string> ->
        Task<UpdateResult>