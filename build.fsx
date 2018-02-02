// --------------------------------------------------------------------------------------
// FAKE build script 
// --------------------------------------------------------------------------------------

#I "packages/build/FAKE/tools"
#r "packages/build/FAKE/tools/FakeLib.dll"
#load "packages/build/SourceLink.Fake/tools/SourceLink.fsx"

open System
open System.IO

open Fake 
open Fake.Git
open Fake.ReleaseNotesHelper
open Fake.AssemblyInfoFile
open Fake.Testing
open SourceLink
open Fake.Testing.Expecto

// --------------------------------------------------------------------------------------
// Information about the project to be used at NuGet and in AssemblyInfo files
// --------------------------------------------------------------------------------------

let project = "FSharp.AWS.DynamoDB"

let gitOwner = "fsprojects"
let gitHome = "https://github.com/" + gitOwner
let gitName = "FSharp.AWS.DynamoDB"
let gitRaw = environVarOrDefault "gitRaw" "https://raw.github.com/" + gitOwner

let remoteTests = environVarOrDefault "RunRemoteTests" "false" |> Boolean.Parse
let emulatorTests = environVarOrDefault "RunEmulatorTests" "false" |> Boolean.Parse

//
//// --------------------------------------------------------------------------------------
//// The rest of the code is standard F# build script 
//// --------------------------------------------------------------------------------------

//// Read release notes & version info from RELEASE_NOTES.md
Environment.CurrentDirectory <- __SOURCE_DIRECTORY__
let release = parseReleaseNotes (File.ReadAllLines "RELEASE_NOTES.md")
let nugetVersion = release.NugetVersion

let mutable dotnet = "dotnet"

Target "InstallDotNetCore" (fun _ ->
    dotnet <- DotNetCli.InstallDotNetSDK "2.1.4"
    Environment.SetEnvironmentVariable("DOTNET_EXE_PATH", dotnet)
)

Target "BuildVersion" (fun _ ->
    Shell.Exec("appveyor", sprintf "UpdateBuild -Version \"%s\"" nugetVersion) |> ignore
)

// Generate assembly info files with the right version & up-to-date information
Target "AssemblyInfo" (fun _ ->
    let attrs =
        [ 
            Attribute.Title project
            Attribute.Product project
            Attribute.Company "Eirik Tsarpalis"
            Attribute.Copyright "Copyright \169 Eirik Tsarpalis 2016"
            Attribute.Version release.AssemblyVersion
            Attribute.FileVersion release.AssemblyVersion
            Attribute.InternalsVisibleTo "FSharp.AWS.DynamoDB.Tests"
        ] 

    !! "**/AssemblyInfo.fs" |> Seq.iter (fun f -> CreateFSharpAssemblyInfo f attrs)
)


// --------------------------------------------------------------------------------------
// Clean build results & restore NuGet packages

Target "Clean" (fun _ ->
    CleanDirs <| !! "./**/bin/"
    CleanDir "./tools/output"
    CleanDir "./temp"
)

//
//// --------------------------------------------------------------------------------------
//// Build library & test project

let configuration = environVarOrDefault "Configuration" "Release"

Target "Build" (fun () ->
    // Build the rest of the project
    DotNetCli.Build (fun p ->
        { p with
            Project = project + ".sln"
            Configuration = configuration
        })
)

// --------------------------------------------------------------------------------------
// Run the unit tests using test runner & kill test runner when complete

Target "RunTests" (fun _ ->
    Unzip "db" "dynamodb_local_latest.zip"
    StartProcess (fun psi ->
        psi.FileName <- "java"
        psi.Arguments <- "-Djava.library.path=./db/DynamoDBLocal_lib -jar ./db/DynamoDBLocal.jar -inMemory"
        ())

    DotNetCli.RunCommand (fun cp -> cp) "run -p tests/FSharp.AWS.DynamoDB.Tests/FSharp.AWS.DynamoDB.Tests.fsproj"

//    killAllCreatedProcesses ()
//    DeleteDir "db"
)

FinalTarget "CloseTestRunner" (fun _ ->  
//    ProcessHelper.killProcess "nunit-agent.exe"
    ()
)

//
//// --------------------------------------------------------------------------------------
//// SourceLink

Target "SourceLink" (fun _ ->
    let baseUrl = sprintf "%s/%s/{0}/%%var2%%" gitRaw project
    [ yield! !! "src/**/*.??proj"]
    |> Seq.iter (fun projFile ->
        let proj = VsProj.LoadRelease projFile
        SourceLink.Index proj.CompilesNotLinked proj.OutputFilePdb __SOURCE_DIRECTORY__ baseUrl
    )
)

//
//// --------------------------------------------------------------------------------------
//// Build a NuGet package

Target "NuGet" (fun _ ->    
    Paket.Pack (fun p -> 
        { p with 
            ToolPath = ".paket/paket.exe" 
            OutputPath = "bin/"
            Version = release.NugetVersion
            ReleaseNotes = toLines release.Notes })
)

Target "NuGetPush" (fun _ -> Paket.Push (fun p -> { p with WorkingDir = "bin/" }))


// Doc generation

Target "GenerateDocs" (fun _ ->
    executeFSIWithArgs "docs/tools" "generate.fsx" ["--define:RELEASE"] [] |> ignore
)

Target "ReleaseDocs" (fun _ ->
    let tempDocsDir = "temp/gh-pages"
    let outputDir = "docs/output"

    CleanDir tempDocsDir
    Repository.cloneSingleBranch "" (gitHome + "/" + gitName + ".git") "gh-pages" tempDocsDir
    
    fullclean tempDocsDir
    ensureDirectory outputDir
    CopyRecursive outputDir tempDocsDir true |> tracefn "%A"
    StageAll tempDocsDir
    Commit tempDocsDir (sprintf "Update generated documentation for version %s" release.NugetVersion)
    Branches.push tempDocsDir
)

// Github Releases

#load "paket-files/build/fsharp/FAKE/modules/Octokit/Octokit.fsx"
open Octokit

Target "ReleaseGitHub" (fun _ ->
    let remote =
        Git.CommandHelper.getGitResult "" "remote -v"
        |> Seq.filter (fun (s: string) -> s.EndsWith("(push)"))
        |> Seq.tryFind (fun (s: string) -> s.Contains(gitOwner + "/" + gitName))
        |> function None -> gitHome + "/" + gitName | Some (s: string) -> s.Split().[0]

    //StageAll ""
    Git.Commit.Commit "" (sprintf "Bump version to %s" release.NugetVersion)
    Branches.pushBranch "" remote (Information.getBranchName "")

    Branches.tag "" release.NugetVersion
    Branches.pushTag "" remote release.NugetVersion

    let client =
        match Environment.GetEnvironmentVariable "OctokitToken" with
        | null -> 
            let user =
                match getBuildParam "github-user" with
                | s when not (String.IsNullOrWhiteSpace s) -> s
                | _ -> getUserInput "Username: "
            let pw =
                match getBuildParam "github-pw" with
                | s when not (String.IsNullOrWhiteSpace s) -> s
                | _ -> getUserPassword "Password: "

            createClient user pw
        | token -> createClientWithToken token

    // release on github
    client
    |> createDraft gitOwner gitName release.NugetVersion (release.SemVer.PreRelease <> None) release.Notes
    |> releaseDraft
    |> Async.RunSynchronously
)

// --------------------------------------------------------------------------------------
// Run all targets by default. Invoke 'build <Target>' to override

Target "Prepare" DoNothing
Target "PrepareRelease" DoNothing
Target "Default" DoNothing
Target "Release" DoNothing

"Clean"
  ==> "InstallDotNetCore"
  ==> "AssemblyInfo"
  ==> "Prepare"
  ==> "Build"
  ==> "RunTests"
  ==> "Default"

"Build"
  ==> "PrepareRelease"
//  ==> "GenerateDocs"
//  ==> "ReleaseDocs"
  ==> "SourceLink"
  ==> "NuGet"
  ==> "NuGetPush"
  ==> "ReleaseGithub"
  ==> "Release"

RunTargetOrDefault "Default"
