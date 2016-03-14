namespace System
open System.Reflection
open System.Runtime.CompilerServices

[<assembly: AssemblyTitleAttribute("FSharp.AWS.DynamoDB")>]
[<assembly: AssemblyProductAttribute("FSharp.AWS.DynamoDB")>]
[<assembly: AssemblyCompanyAttribute("Eirik Tsarpalis")>]
[<assembly: AssemblyCopyrightAttribute("Copyright © Eirik Tsarpalis 2016")>]
[<assembly: AssemblyVersionAttribute("0.0.25")>]
[<assembly: AssemblyFileVersionAttribute("0.0.25")>]
[<assembly: InternalsVisibleToAttribute("FSharp.AWS.DynamoDB.Tests")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.0.25"
