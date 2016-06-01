namespace FSharp.AWS.DynamoDB

open System
open System.IO
open System.Text.RegularExpressions

open Amazon.Util
open Amazon.Runtime

open Microsoft.FSharp.Quotations

/// Collection of extensions for the public API
[<AutoOpen>]
module Extensions =

    /// Precomputes a template expression
    let inline template<'TRecord> = RecordTemplate.Define<'TRecord>()

    /// A conditional which verifies that given item exists
    let inline itemExists<'TRecord> = template<'TRecord>.ItemExists
    /// A conditional which verifies that given item does not exist
    let inline itemDoesNotExist<'TRecord> = template<'TRecord>.ItemDoesNotExist
    
    /// Precomputes a conditional expression
    let inline cond (expr : Expr<'TRecord -> bool>) : ConditionExpression<'TRecord> = 
        template<'TRecord>.PrecomputeConditionalExpr expr

    /// Precomputes an update expression
    let inline update (expr : Expr<'TRecord -> 'TRecord>) : UpdateExpression<'TRecord> = 
        template<'TRecord>.PrecomputeUpdateExpr expr

    /// Precomputes an update operation expression
    let inline updateOp (expr : Expr<'TRecord -> UpdateOp>) : UpdateExpression<'TRecord> = 
        template<'TRecord>.PrecomputeUpdateExpr expr

    /// Precomputes a projection expression
    let inline proj (expr : Expr<'TRecord -> 'TProjection>) : ProjectionExpression<'TRecord, 'TProjection> =
        template<'TRecord>.PrecomputeProjectionExpr<'TProjection> expr


    type AWSCredentials private () =

        // simple recognizer for aws credentials file syntax
        // c.f. http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html
        static let profileRegex =
            Regex("\[(\S+)\]\s+aws_access_key_id\s*=\s*(\S+)\s+aws_secret_access_key\s*=\s*(\S+)", RegexOptions.Compiled)

        /// <summary>
        ///     Recovers a credentials instance from the local environment
        ///     using the the 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' variables.
        /// </summary>
        static member FromEnvironmentVariables() : Amazon.Runtime.AWSCredentials =
            let accessKeyName = "AWS_ACCESS_KEY_ID"
            let secretKeyName = "AWS_SECRET_ACCESS_KEY"
            let getEnv x = Environment.ResolveEnvironmentVariable x

            match getEnv accessKeyName, getEnv secretKeyName with
            | null, null -> sprintf "Undefined environment variables '%s' and '%s'" accessKeyName secretKeyName |> invalidOp
            | null, _ -> sprintf "Undefined environment variable '%s'" accessKeyName |> invalidOp
            | _, null -> sprintf "Undefined environment variable '%s'" secretKeyName |> invalidOp
            | aK, sK  -> new BasicAWSCredentials(aK, sK) :> _

        /// <summary>
        ///     Recover a set of credentials using the local credentials store.
        /// </summary>
        /// <param name="profileName">Credential store profile name. Defaults to 'default' profile.</param>
        static member FromCredentialsStore(?profileName : string) : Amazon.Runtime.AWSCredentials =
            let profileName = defaultArg profileName "default"
            let ok, creds = ProfileManager.TryGetAWSCredentials(profileName)
            if ok then creds
            else
                let credsFile = Path.Combine(getHomePath(), ".aws", "credentials")
                if not <| File.Exists credsFile then
                    sprintf "Could not locate stored credentials profile '%s'." profileName |> invalidOp

                let text = File.ReadAllText credsFile

                let matchingProfile =
                    profileRegex.Matches text
                    |> Seq.cast<Match>
                    |> Seq.map (fun m -> m.Groups.[1].Value, m.Groups.[2].Value, m.Groups.[3].Value)
                    |> Seq.tryFind (fun (pf,_,_) -> pf = profileName)

                match matchingProfile with
                | None -> sprintf "Could not locate stored credentials profile '%s'." profileName |> invalidOp
                | Some (_,aK,sK) -> new BasicAWSCredentials(aK, sK) :> _