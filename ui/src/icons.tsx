import type { ReactNode } from "react";

export function Icon({ name }: { name: string }) {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      {name === "plus" ? <path d="M8 2.5v11M2.5 8h11" /> : null}
      {name === "grid" ? (
        <>
          <rect x="2" y="2" width="5" height="5" rx="1" />
          <rect x="9" y="2" width="5" height="5" rx="1" />
          <rect x="2" y="9" width="5" height="5" rx="1" />
          <rect x="9" y="9" width="5" height="5" rx="1" />
        </>
      ) : null}
      {name === "table" ? (
        <>
          <rect x="2" y="2.5" width="12" height="11" rx="1.5" />
          <path d="M2 6.5h12M2 10h12M6 6.5v7" />
        </>
      ) : null}
      {name === "storage" ? (
        <>
          <ellipse cx="8" cy="4" rx="5" ry="2" />
          <path d="M3 4v8c0 1.1 2.2 2 5 2s5-.9 5-2V4" />
          <path d="M3 8c0 1.1 2.2 2 5 2s5-.9 5-2" />
        </>
      ) : null}
      {name === "sessions" ? (
        <>
          <path d="M3 3.5h10M3 8h10M3 12.5h6" />
        </>
      ) : null}
      {name === "eye" ? (
        <>
          <path d="M1.5 8s2.4-4 6.5-4 6.5 4 6.5 4-2.4 4-6.5 4S1.5 8 1.5 8z" />
          <circle cx="8" cy="8" r="1.6" />
        </>
      ) : null}
      {name === "trash" ? (
        <>
          <path d="M3 4.5h10M6 4.5V3h4v1.5M4.5 4.5l.6 8.2a1 1 0 0 0 1 .8h3.8a1 1 0 0 0 1-.8l.6-8.2" />
        </>
      ) : null}
      {name === "export" ? (
        <>
          <path d="M8 10.5V2.5M5 5.5 8 2.5l3 3" />
          <path d="M3 9.5v3h10v-3" />
        </>
      ) : null}
      {name === "shuffle" ? (
        <>
          <path d="M2.5 4.5h3.2c1.6 0 2.4 1.2 3.3 2.5L11 10h2.5" />
          <path d="M11.5 2.8 14 4.5 11.5 6.2M2.5 11.5h3.2c1.6 0 2.4-1.2 3.3-2.5" />
          <path d="M11.5 9.8 14 11.5 11.5 13.2" />
        </>
      ) : null}
      {name === "chevron-left" ? <path d="M10 3 5 8l5 5" /> : null}
      {name === "chevron-right" ? <path d="M6 3l5 5-5 5" /> : null}
      {name === "copy" ? (
        <>
          <rect x="5.5" y="5.5" width="8" height="8" rx="1.2" />
          <path d="M3.5 10.5V3.2A1.2 1.2 0 0 1 4.7 2h7.3" />
        </>
      ) : null}
      {name === "info" ? (
        <>
          <circle cx="8" cy="8" r="5.5" />
          <path d="M8 7.2V11.2M8 5.1v.2" />
        </>
      ) : null}
      {name === "bell" ? (
        <>
          <path d="M4 11.5h8M6.2 11.5a1.8 1.8 0 0 0 3.6 0" />
          <path d="M5 11.5V7.2a3 3 0 0 1 6 0v4.3" />
          <path d="M8 3.2v.8" />
        </>
      ) : null}
      {name === "bars" ? (
        <>
          <path d="M3 13V8M8 13V3.5M13 13V6" />
          <path d="M2 13.5h12" />
        </>
      ) : null}
    </svg>
  );
}

export function EmptyMark() {
  return (
    <div className="empty-dots" aria-hidden="true">
      {Array.from({ length: 9 }, (_, i) => <span key={i} />)}
    </div>
  );
}
export function IconButton({
  label,
  onClick,
  children,
  disabled,
  pressed,
}: {
  label: string;
  onClick?: () => void;
  children: ReactNode;
  disabled?: boolean;
  pressed?: boolean;
}) {
  return (
    <button className={pressed ? "icon-btn on" : "icon-btn"} type="button" aria-label={label} title={label} aria-pressed={pressed} disabled={disabled} onClick={onClick}>
      {children}
    </button>
  );
}
