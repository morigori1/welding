; AutoHotkey v2 script
; Usage: AutoHotkey.exe scripts/xdw_to_pdf_v2.ahk "INPUT_FILE" "OUTPUT_PDF"

#SingleInstance Force
#Warn
DetectHiddenWindows true

if A_Args.Length < 2 {
    MsgBox "Usage: xdw_to_pdf_v2.ahk INPUT_FILE OUTPUT_PDF"
    ExitApp 2
}

input := A_Args[1]
output := A_Args[2]
; Sanitize paths (trim whitespace/newlines; normalize slashes)
input := Trim(StrReplace(input, "/", "\\"))
output := Trim(StrReplace(output, "/", "\\"))

; Ensure output directory exists (in case caller didn't create it)
SplitPath output, &outName, &outDir
if outDir != "" {
    DirCreate outDir
}

attr := FileExist(input)
if !attr {
    ; try removing stray quotes/spaces
    input := Trim(StrReplace(input, '"'))
}
attr := FileExist(input)
if !attr {
    ExitApp 10 ; input file missing
}
if InStr(attr, "D") {
    ExitApp 12 ; input is directory, not file
}
; Quote the path explicitly for ShellExecute
Run '"' input '"', , , &pid
if !pid {
    MsgBox "Failed to launch: " input, "Error", 16
    ExitApp 3
}

WinWait "ahk_pid " pid,, 10
WinActivate "ahk_pid " pid
Sleep 200
Send "^p"

; Wait for Print dialog, press Enter
if WinWaitActive("ahk_class #32770",, 10) {
    Send "{Enter}"
} else {
    Sleep 500
    Send "^p"
    WinWaitActive "ahk_class #32770",, 8
    Send "{Enter}"
}

; Wait for Save As dialog (Microsoft Print to PDF). If not appears, poll for file creation.
if WinWaitActive("ahk_class #32770",, 15) {
    ok := false
    try {
        ; First try direct control set (classic dialog)
        ControlFocus "Edit1", "A"
        Sleep 100
        ControlSetText "Edit1", output, "A"
        ok := true
    } catch as e {
        ok := false
    }
    if !ok {
        ; Fallback: use accelerator to focus File name and type the path
        Send "!n"
        Sleep 150
        Send "^a"
        Sleep 100
        SendText output
    }
    Sleep 200
    Send "{Enter}"
    ; Overwrite confirmation (if any)
    if WinWait("ahk_class #32770",, 2) {
        ; Try Alt+Y / Enter
        Send "!y"
        Sleep 150
        Send "{Enter}"
    }
}

; Wait for file creation up to 30s
ok := false
Loop 120 {
    if FileExist(output) {
        ok := true
        break
    }
    Sleep 250
}

try {
    if WinExist("ahk_pid " pid)
        WinClose "ahk_pid " pid
} catch as e {
    ; ignore if window already closed
}
if ProcessExist(pid) {
    ; try graceful process close; if it persists, let OS handle cleanup
    ProcessClose pid
    ProcessWaitClose pid, 5
}
ExitApp ok ? 0 : 1
