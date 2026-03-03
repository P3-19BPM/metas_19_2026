Set WshShell = CreateObject("WScript.Shell")
Set Fso = CreateObject("Scripting.FileSystemObject")

scriptDir = Fso.GetParentFolderName(WScript.ScriptFullName)
batPath = Fso.BuildPath(scriptDir, "run_agent_forever.bat")

' 0 = hidden window, False = do not wait
WshShell.Run """" & batPath & """", 0, False

