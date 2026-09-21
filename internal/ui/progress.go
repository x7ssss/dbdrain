package ui

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"golang.org/x/term"
)

// IsTTY returns true if stdout is a terminal.
func IsTTY() bool {
	return term.IsTerminal(int(os.Stdout.Fd()))
}

// Spinner shows an animated spinner on stderr (so it doesn't pollute SQL output).
type Spinner struct {
	message string
	stop    chan struct{}
	done    chan struct{}
}

// NewSpinner creates a new Spinner.
func NewSpinner(message string) *Spinner {
	return &Spinner{
		message: message,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// Start begins the spinner animation in a goroutine.
func (s *Spinner) Start() {
	frames := []string{"⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"}
	go func() {
		defer close(s.done)
		i := 0
		for {
			select {
			case <-s.stop:
				fmt.Fprintf(os.Stderr, "\r%s\r", strings.Repeat(" ", 60))
				return
			default:
				frame := frames[i%len(frames)]
				style := lipgloss.NewStyle().Foreground(lipgloss.Color("#7C3AED"))
				fmt.Fprintf(os.Stderr, "\r%s %s", style.Render(frame), s.message)
				time.Sleep(80 * time.Millisecond)
				i++
			}
		}
	}()
}

// Stop halts the spinner.
func (s *Spinner) Stop() {
	close(s.stop)
	<-s.done
}

// PrintSummary prints a formatted summary table to stderr.
type Summary struct {
	Rows      map[string]int
	Duration  time.Duration
	HasCycles bool
}

// Print renders the summary table.
func (sum *Summary) Print(w io.Writer) {
	headerStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#7C3AED"))

	keyStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#6B7280"))

	valStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#10B981")).
		Bold(true)

	fmt.Fprintln(w)
	fmt.Fprintln(w, headerStyle.Render("  dbdrain  — slice complete"))
	fmt.Fprintln(w)

	for table, count := range sum.Rows {
		fmt.Fprintf(w, "  %s  %s rows\n",
			keyStyle.Render(fmt.Sprintf("%-30s", table)),
			valStyle.Render(fmt.Sprintf("%d", count)),
		)
	}

	fmt.Fprintln(w)
	fmt.Fprintf(w, "  %s  %s\n",
		keyStyle.Render(fmt.Sprintf("%-30s", "Duration")),
		valStyle.Render(sum.Duration.Round(time.Millisecond).String()),
	)
	if sum.HasCycles {
		warnStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#F59E0B")).Bold(true)
		fmt.Fprintf(w, "  %s\n", warnStyle.Render("⚠  Circular FK dependencies detected — SET CONSTRAINTS ALL DEFERRED used"))
	}
	fmt.Fprintln(w)
}
