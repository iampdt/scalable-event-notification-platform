# Renders docs/demo.gif from scripts/demo.sh with charmbracelet/vhs:
#   brew install vhs && docker compose up -d --build && vhs scripts/demo.tape
Output docs/demo.gif
Set Shell "bash"
Set FontSize 14
Set Width 1400
Set Height 900
Set TypingSpeed 40ms
Set Theme "Catppuccin Mocha"

Type "DEMO_PAUSE=1 ./scripts/demo.sh"
Enter
Sleep 75s
