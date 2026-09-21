package ui

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
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
	mu      sync.Mutex
	message string
	stop    chan struct{}
	done    chan struct{}
}

// NewSpinner creates a new Spinner with an initial message.
func NewSpinner(message string) *Spinner {
	return &Spinner{
		message: message,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// UpdateMessage safely updates the spinner message while it is running.
func (s *Spinner) UpdateMessage(msg string) {
	s.mu.Lock()
	s.message = msg
	s.mu.Unlock()
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
				fmt.Fprintf(os.Stderr, "\r%s\r", strings.Repeat(" ", 80))
				return
			default:
				s.mu.Lock()
				msg := s.message
				s.mu.Unlock()

				frame := frames[i%len(frames)]
				style := lipgloss.NewStyle().Foreground(lipgloss.Color("#7C3AED"))
				fmt.Fprintf(os.Stderr, "\r%s %s", style.Render(frame), msg)
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

// Summary holds the data for the final result table.
type Summary struct {
	Rows      map[string]int
	Duration  time.Duration
	HasCycles bool
	// Target is the target DB connection string (non-empty when using --target).
	Target string
}

// Print renders the summary table to w.
func (sum *Summary) Print(w io.Writer) {
	headerStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#7C3AED"))

	keyStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#6B7280"))

	valStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#10B981")).
		Bold(true)

	modeLabel := "SQL emitted"
	if sum.Target != "" {
		modeLabel = "streamed → target"
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, headerStyle.Render("  dbdrain v0.2.0  — slice complete ("+modeLabel+")"))
	fmt.Fprintln(w)

	// Sort tables for deterministic output.
	tables := make([]string, 0, len(sum.Rows))
	for t := range sum.Rows {
		tables = append(tables, t)
	}
	// Simple insertion sort for readability (small N).
	for i := 1; i < len(tables); i++ {
		for j := i; j > 0 && tables[j] < tables[j-1]; j-- {
			tables[j], tables[j-1] = tables[j-1], tables[j]
		}
	}

	for _, table := range tables {
		count := sum.Rows[table]
		fmt.Fprintf(w, "  %s  %s rows\n",
			keyStyle.Render(fmt.Sprintf("%-32s", table)),
			valStyle.Render(fmt.Sprintf("%d", count)),
		)
	}

	fmt.Fprintln(w)
	fmt.Fprintf(w, "  %s  %s\n",
		keyStyle.Render(fmt.Sprintf("%-32s", "Duration")),
		valStyle.Render(sum.Duration.Round(time.Millisecond).String()),
	)
	if sum.HasCycles {
		warnStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#F59E0B")).Bold(true)
		fmt.Fprintf(w, "  %s\n", warnStyle.Render("⚠  Circular FK dependencies detected — SET CONSTRAINTS ALL DEFERRED used"))
	}
	fmt.Fprintln(w)
}
