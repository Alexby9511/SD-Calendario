const COLOR_WHEEL_SIZE = 269;
const COLOR_STEP = 137;

function hashUser(user: string) {
  let hash = 2166136261;
  for (let index = 0; index < user.length; index += 1) {
    hash ^= user.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

export function getSharedCalendarColors(users: string[], currentUser: string) {
  const colors: Record<string, string> = {};
  const usedHues = new Set<number>();

  for (const user of [...new Set(users)].filter((candidate) => candidate !== currentUser).sort()) {
    let hue = hashUser(user) % COLOR_WHEEL_SIZE;
    while (usedHues.has(hue)) {
      hue = (hue + COLOR_STEP) % COLOR_WHEEL_SIZE;
    }
    usedHues.add(hue);
    const displayHue = hue < 100 ? hue : hue < 140 ? hue + 60 : hue + 91;
    colors[user] = `hsl(${displayHue} 72% 64%)`;
  }

  return colors;
}
