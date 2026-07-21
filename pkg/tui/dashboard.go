package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"

	"github.com/jamesits/hfdl/pkg/config"
)

// queueNames index into Snapshot.Queues (contract: 0=meta 1=download
// 2=disk 3=install).
var queueNames = [4]string{"meta", "download", "disk", "install"}

// dashboardView assembles the main tab; lines beyond the terminal height
// are trimmed from the file-table middle, never from header/footer.
func (m model) dashboardView() string {
	s := m.current
	if s == nil {
		s = &Snapshot{}
	}
	lines := []string{
		m.headerLine(s),
		m.queuesLine(s),
	}
	for i := range s.Active {
		lines = append(lines, m.fileLine(s.Active[i]))
	}
	lines = append(lines,
		m.pendingLine(s),
		m.footerLine(s),
		m.dashboardHelpLine(),
	)
	lines = fitLines(lines, m.height, 1, 2)
	for i := range lines {
		lines[i] = m.clamp(lines[i])
	}
	return strings.Join(lines, "\n")
}

func (m model) headerLine(s *Snapshot) string {
	repo := s.Repo
	if repo == "" {
		repo = "—"
	}
	if s.Revision != "" {
		repo += "@" + s.Revision
	}
	if sha := shortSHA(s.CommitSHA); sha != "" {
		repo += " (" + sha + ")"
	}
	parts := []string{
		"hfdl " + m.version,
		repo,
		config.FormatSize(s.BytesDone) + "/" + config.FormatSize(s.BytesTotal),
		humanRate(s.GlobalRate),
		"ETA " + formatETA(s.ETA),
	}
	if flag := m.stateFlag(s); flag != "" {
		parts = append(parts, flag)
	}
	return styleHeader.Render(strings.Join(parts, "  "))
}

// stateFlag surfaces pause/ENOSPC/repo status; m.paused (local key toggle)
// and s.Paused (scheduler truth) are ORed so the UI never lies between the
// keypress and the next snapshot.
func (m model) stateFlag(s *Snapshot) string {
	switch {
	case s.ENOSPCPaused:
		return styleWarn.Render("[ENOSPC]")
	case m.paused || s.Paused:
		return styleWarn.Render("[paused]")
	case s.RepoStatus != "":
		return styleDim.Render("[" + s.RepoStatus + "]")
	}
	return ""
}

func (m model) queuesLine(s *Snapshot) string {
	segs := make([]string, 0, len(s.Queues))
	for i, q := range s.Queues {
		seg := fmt.Sprintf("%s:%d", queueNames[i], q.Depth)
		if q.Detail != "" {
			seg += " " + q.Detail
		}
		if q.InFlight > 0 {
			seg += fmt.Sprintf(" (%d run)", q.InFlight)
		}
		segs = append(segs, seg)
	}
	return "Queues  " + strings.Join(segs, "  ")
}

// fileLine renders one active download; the path absorbs leftover width and
// truncates from the head. When space runs out the right column sheds detail
// in least-important-first order: status, then upstreams.
func (m model) fileLine(fp FileProgress) string {
	barW := min(progressBarWidth, max(3, m.width/8))
	right := fmt.Sprintf("%s/%s  %d conn  %s",
		config.FormatSize(fp.Done), config.FormatSize(fp.Total), fp.Conns, humanRate(fp.Rate))
	if len(fp.Upstreams) > 0 {
		right += "  [" + strings.Join(fp.Upstreams, ",") + "]"
	}
	if fp.Status != "" {
		right += "  " + fp.Status
	}
	pathW := m.width - barW - 1 - lipgloss.Width(right) - 1
	if pathW < minPathWidth && fp.Status != "" {
		right = strings.TrimSuffix(right, "  "+fp.Status)
		pathW = m.width - barW - 1 - lipgloss.Width(right) - 1
	}
	if pathW < minPathWidth && len(fp.Upstreams) > 0 {
		right = right[:strings.LastIndex(right, "  [")]
		pathW = m.width - barW - 1 - lipgloss.Width(right) - 1
	}
	if pathW < 0 {
		// Below min viable width: drop the right column, keep the bar.
		return renderBar(fp.Done, fp.Total, barW) + " " + truncTail(fp.Path, m.width-barW-1)
	}
	return renderBar(fp.Done, fp.Total, barW) + " " + truncTail(fp.Path, pathW) + " " + right
}

func (m model) pendingLine(s *Snapshot) string {
	line := fmt.Sprintf("… pending: %d files", s.PendingCount)
	if len(s.PendingNext) > 0 {
		next := s.PendingNext
		if len(next) > maxPendingNames {
			next = next[:maxPendingNames]
		}
		line += " (next: " + strings.Join(next, ", ") + ", …)"
	}
	return styleDim.Render(line)
}

func (m model) footerLine(s *Snapshot) string {
	bwMax := "∞"
	if s.Limits.MaxBandwidthBps > 0 {
		bwMax = humanRate(float64(s.Limits.MaxBandwidthBps))
	}
	disk := fmt.Sprintf("disk %d/%d%%", int(s.DutyActiveRatio*100), s.DutyLevel)
	if s.DutyMedia != "" {
		disk += "(" + s.DutyMedia + ")"
	}
	segs := []string{
		"bw " + humanRate(s.BandwidthRate) + "/" + bwMax,
		fmt.Sprintf("api %.1f/%ds", s.APIRate, s.Limits.APIIOPS),
		disk,
	}
	if cd := cooldownSummary(s.Cooldowns); cd != "" {
		segs = append(segs, "429: "+cd)
	}
	segs = append(segs,
		fmt.Sprintf("stalls %d retries %d", s.Stalls, s.Retries),
		"salvaged "+strings.Replace(config.FormatSize(s.SalvagedBytes), " ", "", 1))
	return strings.Join(segs, "  ")
}

// cooldownSummary picks the 429/rate-limit cooldown if present, else the
// first one; "" when nothing is cooling down (segment omitted to keep the
// footer inside 80 columns).
func cooldownSummary(cds []CooldownInfo) string {
	if len(cds) == 0 {
		return ""
	}
	pick := cds[0]
	for _, cd := range cds {
		k := strings.ToLower(cd.Kind)
		if strings.Contains(k, "429") || strings.Contains(k, "rate") {
			pick = cd
			break
		}
	}
	if pick.Remaining <= 0 {
		return ""
	}
	return fmt.Sprintf("%s %s", pick.Target, formatETA(pick.Remaining))
}

func (m model) dashboardHelpLine() string {
	help := "q quit  p pause  +/- bandwidth  l logs"
	if n := m.warnBadge(); n > 0 {
		help += styleWarn.Render(fmt.Sprintf(" (⚠%d)", n))
	}
	return styleDim.Render(help)
}
