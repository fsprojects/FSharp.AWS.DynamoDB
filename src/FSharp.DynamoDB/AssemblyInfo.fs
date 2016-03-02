namespace System
open System.Reflection
open System.Runtime.CompilerServices

[<assembly: AssemblyTitleAttribute("FSharp.DynamoDB")>]
[<assembly: AssemblyProductAttribute("FSharp.DynamoDB")>]
[<assembly: AssemblyCompanyAttribute("Eirik Tsarpalis")>]
[<assembly: AssemblyCopyrightAttribute("Copyright © Eirik Tsarpalis 2016")>]
[<assembly: AssemblyVersionAttribute("0.0.21")>]
[<assembly: AssemblyFileVersionAttribute("0.0.21")>]
[<assembly: InternalsVisibleToAttribute("FSharp.DynamoDB.Tests")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.0.21"
