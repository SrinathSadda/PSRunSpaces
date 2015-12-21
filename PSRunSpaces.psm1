#########################################################
# Module Name: PSRunSpaces.psm1
# Module Description: Functions to invoke runspaces with multi-threading
# Author: Srinath Sadda
# Version: 1.0.1
# Updated: 12/20/2015
# Copyright © 2015 Srinath Sadda
#########################################################

# Adds a Microsoft .NET Framework type (a class) to a Windows PowerShell session
# Initialize a custom powershell class 'AsyncPipeline'
If (!('AsyncPipeline' -as [Type])) {
    Add-Type @'
        public class AsyncPipeline {
            public System.Management.Automation.PowerShell Pipeline;
            public System.IAsyncResult AsyncResult;
        }
'@
}

Function Invoke-Async {
    <#
        .SYNOPSIS
            Create a PowerShell pipeline and executes a script block asynchronously.
        .DESCRIPTION
            Create a PowerShell pipeline and executes a script block asynchronously.
        .PARAMETER RunspacePool
            Specify a pool of one or more runspaces, typically created using 'New-RunspacePool' Cmdlet.
            A runspace pool is a collection of runspaces upon which PowerShell pipelines can be executed.
        .PARAMETER ScriptBlock
            Represents a precompiled block of script text that can be used as a single unit.
            A script block is an instance of a Microsoft .NET Framework type (System.Management.Automation.ScriptBlock)
        .PARAMETER Arguments
            A script block can accept arguments and return values.
            The 'Arguments' parameter supplies the values of the variables, in the order that they are listed.
        .EXAMPLE
            $ScriptBlock = { Param($Computer,$Service) Get-Servie -Name $Service -ComputerName $Computer }
            Invoke-Async -RunspacePool $(New-RunSpacePool 10) -ScriptBlock $ScriptBlock -Arguments $Computer,$Service
    #>

    [CmdletBinding()]
    Param (
        [Parameter(Position=0,Mandatory=$True,HelpMessage="Specify a pool of one or more runspaces, typically created using 'New-RunspacePool' Cmdlet:")]
        [ValidateNotNullOrEmpty()]
        [ValidateScript({
            If ($_ -is [System.Management.Automation.Runspaces.RunspacePool]) {
                $True
            }
            Else {
                Throw "OOPS! YOU SPECIFIED AN INCORRECT OBJECT TYPE! THE EXPECTED TYPE IS: [RunspacePool]"
            }
        })]
        [System.Management.Automation.Runspaces.RunspacePool] $RunspacePool,
        
        [Parameter(Position=1,Mandatory=$True,HelpMessage="Represents a precompiled block of script text that can be used as a single unit:")]
        [ValidateNotNullOrEmpty()]
        [ValidateScript({
            If ($_ -is [System.Management.Automation.ScriptBlock]) {
                $True
            }
            Else {
                Throw "OOPS! YOU SPECIFIED AN INCORRECT OBJECT TYPE! THE EXPECTED TYPE IS: [ScriptBlock]"
            }
        })]
        [System.Management.Automation.ScriptBlock] $ScriptBlock,
        
        [Parameter(Position=2,Mandatory=$False,HelpMessage="A script block can accept arguments and return values. The 'Arguments' parameter supplies the values of the variables, in the order that they are listed:")]
        [ValidateNotNullOrEmpty()]
        [Object[]] $Arguments
    )

    Try {
        # Initializes a new instance of the PowerShell class with an empty pipeline.
        $Pipeline = [System.Management.Automation.PowerShell]::Create()
        
        # Sets the runspace pool used by the PowerShell object.
        # A runspace from this pool is used whenever the PowerShell object pipeline is invoked.
        $Pipeline.RunspacePool = $RunspacePool
        
        # NOTE: Out-Null - Deletes output instead of sending it down the pipeline.
        # Adds a script to the end of the pipeline of the PowerShell object.
        $Pipeline.AddScript($ScriptBlock) | Out-Null
        
        Foreach($Arg in $Arguments) {
            # NOTE: Out-Null - Deletes output instead of sending it down the pipeline.
            # Adds an argument for a positional parameter of a command without specifying the parameter name.
            If ($Arg -is [Object[]]) {
                Foreach($Arg_ in $Arg) {
                    $Pipeline.AddArgument($Arg_) | Out-Null
                }
            }
            Else {
                $Pipeline.AddArgument($Arg) | Out-Null
            }
        }

        # Asynchronously runs the commands of the PowerShell object pipeline.
        $AsyncResult = $Pipeline.BeginInvoke()

        # Creates a AsyncPipeline object.
        $Status = New-Object AsyncPipeline -ErrorAction Stop -ErrorVariable AsyncPipeline_

        If (!$AsyncPipeline_) {
            $Status.Pipeline = $Pipeline
            $Status.AsyncResult = $AsyncResult
        
            If ($Status) {
                # Returns the status of AsyncPipeline
                Return $Status
            }
        }
    }
    Catch {
        # Capture an exception
        $E = $_.Exception.Message
        Return $E
    }
}

Function New-RunSpacePool {
    <#
        .SYNOPSIS
            Creates a runspace pool.
        .DESCRIPTION
            Creates a pool of runspaces that specifies the minimum and maximum number of opened runspaces for the pool.
        .PARAMETER MaxThreads
            Defines the maximum number of pipelines that can be concurrently (asynchronously) executed on the pool.
            The number of available pools determined the maximum number of processes that can be running concurrently.
        .PARAMETER MTA
            Create runspaces in a multi-threaded apartment. It is not recommended to use this option unless absolutely necessary.
        .EXAMPLE
            Creates a pool of 10 runspaces.
            $RSPool = New-RunSpacePool -MaxThreads 10
    #>

    [CmdletBinding()]
    Param (
        [Parameter(Position=0,Mandatory=$True,HelpMessage="Specify maximum no. of threads (maximum is 64):")]
        [ValidateNotNullOrEmpty()]
        [ValidateRange(1,64)]
        [Int16] $MaxThreads,

        [Parameter(Position=1,Mandatory=$False,HelpMessage="Specify this switch to create runspaces in a multi-threaded apartment:")]
        [Switch] $MTA
    )

    Try {
        # Creates a pool of runspaces that specifies the minimum and maximum number of opened runspaces for the pool
        $RSPool = [RunspaceFactory]::CreateRunspacePool(1, $MaxThreads)
        
        # NOTE: Runspace.ApartmentState property must be set before the runspace is opened.
        # Specify the apartment state of the thread used to run commands in the runspace.
        
        If ($MTA) {
            # The thread will create and enter a multi-threaded apartment.
            $RSPool.ApartmentState = 'MTA'
        }
        Else {
            # The thread will create and enter a single-threaded apartment.
            $RSPool.ApartmentState = 'STA'
        }

        # Open runspaces.
        $RSPool.Open()
        
        # Returns a pool of runspaces.
        Return $RSPool
    }
    Catch {
        # Capture an exception
        $E = $_.Exception.Message
        Return $E
    }
}

Function Get-AsyncInfo {
    <#
        .SYNOPSIS
            Receives the results of one or more asynchronous pipelines.
        .DESCRIPTION
            Receives the results of one or more asynchronous pipelines running in a separate runspaces.
        .PARAMETER Pipelines
            An array of AsyncPipleine objects, typically returned by 'Invoke-Async' Cmdlet.
        .PARAMETER Progress
            An optional switch to display a progress bar that depicts the status of a running command.
        .EXAMPLE
            $ScriptBlock = { Param($Computer,$Service) Get-Servie -Name $Service -ComputerName $Computer }
            $AsyncPipelines = Invoke-Async -RunspacePool $(New-RunSpacePool 10) -ScriptBlock $ScriptBlock -Arguments $Computer,$Service
            
            Get-AsyncInfo -Pipelines $AsyncPipelines -Progress
        .NOTES
            Since it is unknown what exists in the results stream of the pipeline, this function will not have a standard return type.
    #>

    [CmdletBinding()]
    Param (
        [Parameter(Position=0,Mandatory=$True,HelpMessage="Specify an array of AsyncPipleine objects, typically returned by Invoke-Async cmdlet:")]
        [ValidateNotNullOrEmpty()]
        [AsyncPipeline[]] $Pipelines,
	    
        [Parameter(Position=1,Mandatory=$False,HelpMessage="Specify this switch to display a progress bar that depicts the status of a running command:")]
        [Switch] $Progress
    )

    # Counter for Write-Progress
    $I = 1

    ForEach ($Pipeline in $Pipelines) {
        Try {
            # NOTE:
            # Pipeline.EndInvoke - Waits for the pending asynchronous BeginInvoke call to be completed and then returns the results of the call.
            # AsyncResult - The IAsyncResult interface returned by the BeginInvoke call. This interface represents the status of the call.
            
            # Get the results from the IAsyncResult object.
            $Pipeline.Pipeline.EndInvoke($Pipeline.AsyncResult)
            
            # Capture and throw pipeline stream errors.
            If ($Pipeline.Pipeline.Streams.Error) {
                Throw $Pipeline.Pipeline.Streams.Error
            }
        }
        Catch {
            # Capture an exception
            $E = $_.Exception.Message
            Return $E
        }
        Finally {
            # Releases all resources used by the PowerShell object.
            $Pipeline.Pipeline.Dispose()

            If ($Progress) {
                # Displays a progress bar that depicts the status of a running command.
                Write-Progress -Activity 'Receiving Results' -PercentComplete $(($I/$Pipelines.Length) * 100) -Status 'Percent Complete'
            }
        
            # Increment counter for Write-Progress
            $I++
        }
    }
}

Function Get-AsyncStatus {
    <#
        .SYNOPSIS
            Receives the status of one or more asynchronous pipelines.
        .DESCRIPTION
            Receives the status of one or more asynchronous pipelines.
        .PARAMETER Pipelines
            An array of AsyncPipleine objects, typically returned by 'Invoke-Async' Cmdlet.
        .EXAMPLE
            $ScriptBlock = { Param($Computer,$Service) Get-Servie -Name $Service -ComputerName $Computer }
            $AsyncPipelines = Invoke-Async -RunspacePool $(New-RunSpacePool 10) -ScriptBlock $ScriptBlock -Arguments $Computer,$Service

            $Status = Get-AsyncStatus -Pipelines $AsyncPipelines
        .EXAMPLE
            $ScriptBlock = { Param($Computer,$Service) Get-Servie -Name $Service -ComputerName $Computer }
            
            $Status = Get-AsyncStatus -Pipelines $(Invoke-Async -RunspacePool $(New-RunSpacePool 10) -ScriptBlock $ScriptBlock -Arguments $Computer,$Service)
    #>

    [CmdletBinding()]
    Param (
        [Parameter(Position=0,Mandatory=$True,HelpMessage="Specify an array of AsyncPipleine objects, typically returned by Invoke-Async Cmdlet:")]
        [ValidateNotNullOrEmpty()]
        [AsyncPipeline[]] $Pipelines
    )

    [Array] $AsyncStatus = @()

    Try {
        ForEach ($Pipeline in $Pipelines) {
            [HashTable] $HT = @{
                'Instance ID' = $Pipeline.Pipeline.Instance_Id
                'Status' = $Pipeline.Pipeline.InvocationStateInfo.State
                'Reason' = $Pipeline.Pipeline.InvocationStateInfo.Reason
                'Completed' = $Pipeline.AsyncResult.IsCompleted
                'Async State' = $Pipeline.AsyncResult.AsyncState
                'Error' = $Pipeline.Pipeline.Streams.Error
            }

            # Create a custom PSObject to hold powershell pipeline info
            $PipelineInfo = New-Object -TypeName PSObject -Property $HT -ErrorAction Stop -ErrorVariable HT_
            If (!$HT_) {
                $AsyncStatus += $PipelineInfo
            }
        }

        # Returns the status of one or more asynchronous pipelines.
        Return $AsyncStatus
    }
    Catch {
        # Capture an exception
        $E = $_.Exception.Message
        Return $E
    }
}

Function Invoke-PSRunSpaces {
    <#
        .SYNOPSIS
            Executes a set of parameterized script blocks asynchronously using runspaces and returns the resulting data.
        .DESCRIPTION
            Encapsulates generic logic for using Powershell background runspaces to execute parameterized script blocks in an efficient multi-threaded fashion.
        .PARAMETER Objects
            List of objects.
        .PARAMETER ScriptBlock
            Represents a precompiled block of script text that can be used as a single unit.
            ScriptBlock should contain one or more parameters.
            A script block is an instance of a Microsoft .NET Framework type (System.Management.Automation.ScriptBlock)
        .PARAMETER Arguments
            A script block can accept arguments and return values.
            The 'Arguments' parameter supplies the values of the variables, in the order that they are listed.
        .PARAMETER MaxThreads
            The maximum number of concurrent threads to use. The default value is equal to no. of specified servers. Maximum is 64.
        .PARAMETER Progress
            An optional switch to display a progress bar that depicts the status of a running command.
        .PARAMETER SendObjectInstance
        .EXAMPLE
            Opens a separate runspace for each object specified in the $Objects variable and executes the ScriptBlock with the specified no. of Arguments.
            
            $Service = "Netlogon"
            $Objects = @('S1.Contoso.com','S2.Contoso.com')
            $ScriptBlock = {
                Param(
                    $Object,
                    $Service
                )
                Get-Servie -Name $Service -ComputerName $Object
            }
            
            Invoke-PSRunSpaces -Objects $Objects -ScriptBlock $ScriptBlock -Arguments $Service -SendObjectInstance
    #>

    [CmdletBinding()]
    Param (
        [Parameter(Position=0,Mandatory=$True,HelpMessage="Specify a list of objects:")]
        [ValidateNotNullOrEmpty()]
        $Objects,

        [Parameter(Position=1,Mandatory=$True,HelpMessage="A script block is an instance of a Microsoft .NET Framework type (System.Management.Automation.ScriptBlock):")]
        [ValidateNotNullOrEmpty()]
        [ValidateScript({
            If ($_ -is [System.Management.Automation.ScriptBlock]) {
                $True
            }
            Else {
                Throw "YOU SPECIFIED A WRONG OBJECT TYPE! THE EXPECTED TYPE IS: [ScriptBlock]"
            }
        })]
        [System.Management.Automation.ScriptBlock] $ScriptBlock,

        [Parameter(Position=2,Mandatory=$False,HelpMessage="A script block can accept arguments and return values:")]
        [ValidateNotNullOrEmpty()]
        [Alias("Args")]
        [Object[]] $Arguments,

        [Parameter(Position=3,Mandatory=$False,HelpMessage="Specify maximum no. of threads (maximum is 32):")]
        [ValidateNotNullOrEmpty()]
        [ValidateRange(1,32)]
        [Int16] $MaxThreads,

        [Parameter(Position=4,Mandatory=$False,HelpMessage="Specify this switch to display a progress bar that depicts the status of a running command:")]
        [Switch] $Progress,

        [Parameter(Position=5,Mandatory=$False)]
        [Switch] $SendObjectInstance
    )

    Try {
        # Array variable to store a list of pipelines.
        $AsyncPipelines = @()

        # Array variable to store result from all runspaces.
        $Status = @()
        
        # Create a pool with specified no. of runspaces.
        If (!$MaxThreads) {
            If ($Objects.Count -gt 32) {
                $MaxThreads = 32
            }
            Else {
                $MaxThreads = $Objects.Count
            }
        }
        
        # Create a pool with sufficient no. of runspaces (based on no. of objects)
        $RSPool = New-RunSpacePool -MaxThreads $MaxThreads

        ForEach ($Object in $Objects) {
            # Suspends the activity for the specified period of time until a runspace is available.
            While ($($RSPool.GetAvailableRunspaces()) -le 0) {
                Start-Sleep -Milliseconds 200
            }
            
            # Create a PowerShell pipeline and executes a script block asynchronously.
            If (($Arguments) -and ($SendObjectInstance)) { $AsyncPipelines += Invoke-Async -RunSpacePool $RSPool -ScriptBlock $ScriptBlock -Arguments $Object,@($Arguments) }
            ElseIf (($Arguments) -and (!$SendObjectInstance)) { $AsyncPipelines += Invoke-Async -RunSpacePool $RSPool -ScriptBlock $ScriptBlock -Arguments @($Arguments) }
            ElseIf ((!$Arguments) -and ($SendObjectInstance)) { $AsyncPipelines += Invoke-Async -RunSpacePool $RSPool -ScriptBlock $ScriptBlock -Arguments $Object }
            Else { $AsyncPipelines += Invoke-Async -RunSpacePool $RSPool -ScriptBlock $ScriptBlock }
        }

        # Waits for the pending asynchronous BeginInvoke call to be completed in all opened runspaces.
        While ($(Get-AsyncStatus -Pipelines $AsyncPipelines | Where-Object {$_.Status -eq 'Running'}).Count -gt 0) {
            Start-Sleep -Milliseconds 200
        }

        # Get the results of all pipelines running in separate runspaces.
        If ($Progress) { $Status += Get-AsyncInfo -Pipelines $AsyncPipelines -Progress }
        Else { $Status += Get-AsyncInfo -Pipelines $AsyncPipelines }

        # Return data.
        Return $Status
    }
    Catch {
        # Capture an exception
        Write-Error -Exception $_.Exception -Message $_.Exception.Message
    }
    Finally {
        # Releases all resources used by the PowerShell object.
        $RSPool.Dispose()
    }
}