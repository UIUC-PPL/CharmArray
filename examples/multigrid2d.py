"""2D Multigrid V-cycle solver for the Poisson equation.

Solves -Laplacian(u) = f on the unit square [0,1]^2 with Dirichlet
boundary conditions using a geometric multigrid V-cycle with:
  - Weighted Jacobi smoothing
  - Full-weighting restriction (fine -> coarse)
  - Bilinear prolongation (coarse -> fine)

Usage:
    python multigrid2d.py
"""

import argparse
import charmnumeric as cnp
from charmtyles.core import execute, set_auto_flush
import numpy as np


def jacobi_smooth(u, f, h2, n_smooth, omega=2.0 / 3.0):
    """Apply n_smooth weighted-Jacobi iterations to smooth u."""
    for _ in range(n_smooth):
        jacobi_update = 0.25 * (
            u[:-2, 1:-1] + u[2:, 1:-1]
            + u[1:-1, :-2] + u[1:-1, 2:]
            + h2 * f[1:-1, 1:-1]
        )
        u[1:-1, 1:-1] = (
            (1.0 - omega) * u[1:-1, 1:-1] + omega * jacobi_update
        )


def compute_residual(u, f, h2):
    """Return r = f - (-Lap(u)/h2), i.e. r = f + Lap(u)/h2.

    Discrete Laplacian: Lap(u)[i,j] = (u[i-1,j]+u[i+1,j]+u[i,j-1]+u[i,j+1]-4*u[i,j]) / h^2
    For -Lap(u) = f, the residual is r = f - (-Lap(u)/h2) = f - (4*u - neighbors) / h2.
    """
    r = cnp.zeros(u.shape, dtype=cnp.float64)
    r[1:-1, 1:-1] = f[1:-1, 1:-1] - (1.0 / h2) * (
        4.0 * u[1:-1, 1:-1]
        - u[:-2, 1:-1] - u[2:, 1:-1]
        - u[1:-1, :-2] - u[1:-1, 2:]
    )
    return r


def compute_residual_numpy(u, f, h2):
    """NumPy reference for the residual operator."""
    r = np.zeros_like(u)
    r[1:-1, 1:-1] = f[1:-1, 1:-1] - (1.0 / h2) * (
        4.0 * u[1:-1, 1:-1]
        - u[:-2, 1:-1] - u[2:, 1:-1]
        - u[1:-1, :-2] - u[1:-1, 2:]
    )
    return r


def restrict_full_weighting(r_fine, r_coarse):
    """Restrict a fine-grid residual with the standard 9-point stencil."""
    r_coarse[:, :] = 0.0
    r_coarse[1:-1, 1:-1] = (
        0.25 * r_fine[2:-2:2, 2:-2:2]
        + 0.125 * (
            r_fine[1:-3:2, 2:-2:2] + r_fine[3:-1:2, 2:-2:2]
            + r_fine[2:-2:2, 1:-3:2] + r_fine[2:-2:2, 3:-1:2]
        )
        + 0.0625 * (
            r_fine[1:-3:2, 1:-3:2] + r_fine[1:-3:2, 3:-1:2]
            + r_fine[3:-1:2, 1:-3:2] + r_fine[3:-1:2, 3:-1:2]
        )
    )


def restrict_full_weighting_numpy(r_fine):
    """NumPy reference for the 9-point full-weighting restriction."""
    n_coarse = (r_fine.shape[0] - 1) // 2 + 1
    r_coarse = np.zeros((n_coarse, n_coarse), dtype=r_fine.dtype)
    r_coarse[1:-1, 1:-1] = (
        0.25 * r_fine[2:-2:2, 2:-2:2]
        + 0.125 * (
            r_fine[1:-3:2, 2:-2:2] + r_fine[3:-1:2, 2:-2:2]
            + r_fine[2:-2:2, 1:-3:2] + r_fine[2:-2:2, 3:-1:2]
        )
        + 0.0625 * (
            r_fine[1:-3:2, 1:-3:2] + r_fine[1:-3:2, 3:-1:2]
            + r_fine[3:-1:2, 1:-3:2] + r_fine[3:-1:2, 3:-1:2]
        )
    )
    return r_coarse


def validation_pattern_ops(n):
    """A deterministic pattern that crosses chare boundaries and parities."""
    quarter = n // 4
    three_quarter = 3 * n // 4 + 1
    eighth = n // 8
    three_eighth = 3 * n // 8
    seven_eighth = 7 * n // 8 + 1
    mid = n // 2

    return [
        ((slice(None), slice(None)), 0.0),
        ((slice(1, n - 1), slice(1, n - 1)), 1.0),
        ((slice(1, n - 1, 2), slice(1, n - 1, 2)), 2.0),
        ((slice(2, n - 2, 2), slice(1, n - 1, 2)), -1.0),
        ((slice(1, n - 1, 2), slice(2, n - 2, 2)), 0.5),
        ((slice(2, n - 2, 2), slice(2, n - 2, 2)), -0.25),
        ((slice(quarter, three_quarter), slice(quarter, three_quarter)), 3.0),
        ((slice(three_eighth, seven_eighth, 2), slice(eighth, 5 * n // 8 + 1, 2)), -2.0),
        ((slice(eighth, seven_eighth, 4), slice(5 * n // 16, 15 * n // 16, 2)), 0.75),
        ((slice(mid - 2, mid + 3), slice(mid - 2, mid + 3)), -4.0),
    ]


def apply_validation_pattern_numpy(arr):
    """Fill a NumPy array with the deterministic validation pattern."""
    for key, value in validation_pattern_ops(arr.shape[0]):
        arr[key] = value


def apply_validation_pattern_backend(arr):
    """Fill a backend array with the deterministic validation pattern."""
    for key, value in validation_pattern_ops(arr.shape[0]):
        arr[key] = float(value)


def summarize_diff(label, backend, reference):
    """Print max/L2 error stats for two arrays and return the max abs error."""
    diff = backend - reference
    max_abs = float(np.max(np.abs(diff)))
    l2_err = float(np.linalg.norm(diff))
    worst = np.unravel_index(np.argmax(np.abs(diff)), diff.shape)
    print(label)
    print(f"  max abs error = {max_abs:.6e}")
    print(f"  l2 error      = {l2_err:.6e}")
    print(
        f"  worst entry   = {worst}: backend={backend[worst]:.12e}, "
        f"numpy={reference[worst]:.12e}, diff={diff[worst]:.12e}"
    )
    return max_abs, l2_err


def prolongate_and_correct(e_coarse, u_fine):
    """Prolongate a coarse correction with bilinear interpolation."""
    n_fine = u_fine.shape[0]
    e_fine = cnp.zeros((n_fine, n_fine), dtype=cnp.float64)
    e_fine[2:-2:2, 2:-2:2] = e_coarse[1:-1, 1:-1]
    e_fine[1:-1:2, 2:-2:2] = 0.5 * (
        e_coarse[:-1, 1:-1] + e_coarse[1:, 1:-1]
    )
    e_fine[2:-2:2, 1:-1:2] = 0.5 * (
        e_coarse[1:-1, :-1] + e_coarse[1:-1, 1:]
    )
    e_fine[1:-1:2, 1:-1:2] = 0.25 * (
        e_coarse[:-1, :-1] + e_coarse[1:, :-1]
        + e_coarse[:-1, 1:] + e_coarse[1:, 1:]
    )
    u_fine[1:-1, 1:-1] = u_fine[1:-1, 1:-1] + e_fine[1:-1, 1:-1]


def prolongate_numpy(e_coarse):
    """NumPy reference for bilinear prolongation from coarse to fine grid."""
    n_fine = 2 * (e_coarse.shape[0] - 1) + 1
    e_fine = np.zeros((n_fine, n_fine), dtype=e_coarse.dtype)
    e_fine[2:-2:2, 2:-2:2] = e_coarse[1:-1, 1:-1]
    e_fine[1:-1:2, 2:-2:2] = 0.5 * (
        e_coarse[:-1, 1:-1] + e_coarse[1:, 1:-1]
    )
    e_fine[2:-2:2, 1:-1:2] = 0.5 * (
        e_coarse[1:-1, :-1] + e_coarse[1:-1, 1:]
    )
    e_fine[1:-1:2, 1:-1:2] = 0.25 * (
        e_coarse[:-1, :-1] + e_coarse[1:, :-1]
        + e_coarse[:-1, 1:] + e_coarse[1:, 1:]
    )
    return e_fine


def jacobi_smooth_numpy(u, f, h2, n_smooth, omega=2.0 / 3.0):
    """NumPy reference for weighted Jacobi smoothing."""
    out = np.array(u, copy=True)
    for _ in range(n_smooth):
        jacobi_update = 0.25 * (
            out[:-2, 1:-1] + out[2:, 1:-1]
            + out[1:-1, :-2] + out[1:-1, 2:]
            + h2 * f[1:-1, 1:-1]
        )
        out[1:-1, 1:-1] = (
            (1.0 - omega) * out[1:-1, 1:-1] + omega * jacobi_update
        )
    return out


def snapshot_backend(arr):
    """Materialize a full-array backend snapshot without flushing early."""
    snap = cnp.zeros(arr.shape, dtype=cnp.float64)
    snap[:, :] = arr[:, :]
    return snap


def vcycle_numpy(u, f, h, n_pre, n_post, coarse_sweeps=400):
    """NumPy reference for one multigrid V-cycle."""
    out = np.array(u, copy=True)
    n = out.shape[0]
    h2 = h * h
    omega = 2.0 / 3.0

    if n <= 5:
        return jacobi_smooth_numpy(out, f, h2, coarse_sweeps, omega)

    out = jacobi_smooth_numpy(out, f, h2, n_pre, omega)
    r = compute_residual_numpy(out, f, h2)
    r_coarse = restrict_full_weighting_numpy(r)
    e_coarse = np.zeros_like(r_coarse)
    e_coarse = vcycle_numpy(e_coarse, r_coarse, 2.0 * h, n_pre, n_post, coarse_sweeps)
    out = out + prolongate_numpy(e_coarse)
    out = jacobi_smooth_numpy(out, f, h2, n_post, omega)
    return out


def vcycles_numpy(u, f, h, cycles, n_pre, n_post, coarse_sweeps=400):
    """NumPy reference for multiple V-cycles applied back-to-back."""
    out = np.array(u, copy=True)
    for _ in range(cycles):
        out = vcycle_numpy(out, f, h, n_pre, n_post, coarse_sweeps)
    return out


def vcycle_numpy_capture(u, f, h, n_pre, n_post, coarse_sweeps=400):
    """NumPy reference for one V-cycle plus top-level intermediate snapshots."""
    out = np.array(u, copy=True)
    n = out.shape[0]
    h2 = h * h
    omega = 2.0 / 3.0

    if n <= 5:
        coarse = jacobi_smooth_numpy(out, f, h2, coarse_sweeps, omega)
        return {
            "u_pre": coarse,
            "r": np.zeros_like(coarse),
            "r_coarse": np.zeros((0, 0), dtype=coarse.dtype),
            "e_coarse": coarse,
            "u_corr": coarse,
            "u_final": coarse,
        }

    out = jacobi_smooth_numpy(out, f, h2, n_pre, omega)
    u_pre = np.array(out, copy=True)
    r = compute_residual_numpy(out, f, h2)
    r_coarse = restrict_full_weighting_numpy(r)
    e_coarse = np.zeros_like(r_coarse)
    e_coarse = vcycle_numpy(e_coarse, r_coarse, 2.0 * h, n_pre, n_post, coarse_sweeps)
    out = out + prolongate_numpy(e_coarse)
    u_corr = np.array(out, copy=True)
    out = jacobi_smooth_numpy(out, f, h2, n_post, omega)
    return {
        "u_pre": u_pre,
        "r": r,
        "r_coarse": r_coarse,
        "e_coarse": e_coarse,
        "u_corr": u_corr,
        "u_final": out,
    }


def vcycle(u, f, h, n_pre, n_post, coarse_sweeps=400):
    """Perform one V-cycle for -Lap(u) = f on a grid with spacing h."""
    n = u.shape[0]
    h2 = h * h
    omega = 2.0 / 3.0

    if n <= 5:
        jacobi_smooth(u, f, h2, coarse_sweeps, omega)
        return

    # Pre-smoothing
    jacobi_smooth(u, f, h2, n_pre, omega)

    # Compute residual on fine grid
    #r = cnp.zeros(u.shape, dtype=cnp.float64)
    r = compute_residual(u, f, h2)

    # Restrict residual to coarse grid
    n_coarse = (n - 1) // 2 + 1
    r_coarse = cnp.zeros((n_coarse, n_coarse), dtype=cnp.float64)
    restrict_full_weighting(r, r_coarse)

    # Solve the coarse error equation recursively.
    e_coarse = cnp.zeros((n_coarse, n_coarse), dtype=cnp.float64)
    vcycle(e_coarse, r_coarse, 2.0 * h, n_pre, n_post, coarse_sweeps)

    # Prolongate and correct
    prolongate_and_correct(e_coarse, u)

    # Post-smoothing
    jacobi_smooth(u, f, h2, n_post, omega)


def vcycle_capture_backend(u, f, h, n_pre, n_post, coarse_sweeps=400):
    """Build one backend V-cycle DAG and retain top-level intermediate arrays."""
    n = u.shape[0]
    h2 = h * h
    omega = 2.0 / 3.0

    if n <= 5:
        jacobi_smooth(u, f, h2, coarse_sweeps, omega)
        return {
            "u_pre": snapshot_backend(u),
            "r": None,
            "r_coarse": None,
            "e_coarse": snapshot_backend(u),
            "u_corr": snapshot_backend(u),
            "u_final": u,
        }

    jacobi_smooth(u, f, h2, n_pre, omega)
    u_pre = snapshot_backend(u)

    r = compute_residual(u, f, h2)

    n_coarse = (n - 1) // 2 + 1
    r_coarse = cnp.zeros((n_coarse, n_coarse), dtype=cnp.float64)
    restrict_full_weighting(r, r_coarse)

    e_coarse = cnp.zeros((n_coarse, n_coarse), dtype=cnp.float64)
    vcycle(e_coarse, r_coarse, 2.0 * h, n_pre, n_post, coarse_sweeps)
    e_coarse_snapshot = snapshot_backend(e_coarse)

    prolongate_and_correct(e_coarse, u)
    u_corr = snapshot_backend(u)

    jacobi_smooth(u, f, h2, n_post, omega)
    return {
        "u_pre": u_pre,
        "r": r,
        "r_coarse": r_coarse,
        "e_coarse": e_coarse_snapshot,
        "u_corr": u_corr,
        "u_final": u,
    }


def validate_restriction(interface, n):
    """Compare backend restriction against a NumPy reference."""
    fine_ref = np.zeros((n, n), dtype=np.float64)
    apply_validation_pattern_numpy(fine_ref)
    coarse_ref = restrict_full_weighting_numpy(fine_ref)

    fine_probe = cnp.zeros((n, n), dtype=cnp.float64)
    apply_validation_pattern_backend(fine_probe)
    fine_backend = fine_probe.get(interface)
    print("")
    fine_max_abs, fine_l2 = summarize_diff(
        "Fine-grid validation pattern against NumPy", fine_backend, fine_ref
    )

    coarse_from_backend_fine = restrict_full_weighting_numpy(fine_backend)

    fine = cnp.zeros((n, n), dtype=cnp.float64)
    coarse = cnp.zeros(((n - 1) // 2 + 1, (n - 1) // 2 + 1), dtype=cnp.float64)
    apply_validation_pattern_backend(fine)
    restrict_full_weighting(fine, coarse)
    coarse_backend = coarse.get(interface)

    print("")
    coarse_max_abs, coarse_l2 = summarize_diff(
        "Restriction validation against NumPy", coarse_backend, coarse_ref
    )
    print("")
    summarize_diff(
        "Restriction validation against backend fine-grid snapshot",
        coarse_backend,
        coarse_from_backend_fine,
    )

    return max(fine_max_abs, coarse_max_abs), max(fine_l2, coarse_l2)


def validate_prolongation(interface, n):
    """Compare backend prolongation/correction against a NumPy reference."""
    if (n - 1) % 2 != 0:
        raise ValueError("validate-prolong requires n = 2^k + 1")

    n_coarse = (n - 1) // 2 + 1
    coarse_ref = np.zeros((n_coarse, n_coarse), dtype=np.float64)
    apply_validation_pattern_numpy(coarse_ref)
    fine_ref = prolongate_numpy(coarse_ref)

    e_coarse = cnp.zeros((n_coarse, n_coarse), dtype=cnp.float64)
    u_fine = cnp.zeros((n, n), dtype=cnp.float64)
    apply_validation_pattern_backend(e_coarse)
    u_fine[:, :] = 0.0
    prolongate_and_correct(e_coarse, u_fine)
    fine_backend = u_fine.get(interface)

    print("")
    return summarize_diff(
        "Prolongation validation against NumPy", fine_backend, fine_ref
    )


def validate_jacobi(interface, n, n_smooth=1):
    """Compare backend weighted Jacobi against a NumPy reference."""
    h = 1.0 / (n - 1)
    h2 = h * h
    omega = 2.0 / 3.0

    u_ref = np.zeros((n, n), dtype=np.float64)
    f_ref = np.zeros((n, n), dtype=np.float64)
    apply_validation_pattern_numpy(u_ref)
    apply_validation_pattern_numpy(f_ref)
    u_expected = jacobi_smooth_numpy(u_ref, f_ref, h2, n_smooth, omega)

    u_backend = cnp.zeros((n, n), dtype=cnp.float64)
    f_backend = cnp.zeros((n, n), dtype=cnp.float64)
    apply_validation_pattern_backend(u_backend)
    apply_validation_pattern_backend(f_backend)
    jacobi_smooth(u_backend, f_backend, h2, n_smooth, omega)
    u_actual = u_backend.get(interface)

    print("")
    return summarize_diff(
        f"Jacobi validation ({n_smooth} sweep{'s' if n_smooth != 1 else ''}) against NumPy",
        u_actual,
        u_expected,
    )


def validate_residual(interface, n):
    """Compare backend residual formation against a NumPy reference."""
    h = 1.0 / (n - 1)
    h2 = h * h

    u_ref = np.zeros((n, n), dtype=np.float64)
    f_ref = np.zeros((n, n), dtype=np.float64)
    apply_validation_pattern_numpy(u_ref)
    apply_validation_pattern_numpy(f_ref)
    r_expected = compute_residual_numpy(u_ref, f_ref, h2)

    u_backend = cnp.zeros((n, n), dtype=cnp.float64)
    f_backend = cnp.zeros((n, n), dtype=cnp.float64)
    apply_validation_pattern_backend(u_backend)
    apply_validation_pattern_backend(f_backend)
    r_backend = compute_residual(u_backend, f_backend, h2)
    r_actual = r_backend.get(interface)

    print("")
    return summarize_diff(
        "Residual validation against NumPy",
        r_actual,
        r_expected,
    )


def validate_vcycle(interface, n, n_pre, n_post, coarse_sweeps):
    """Compare one backend V-cycle against a NumPy reference."""
    h = 1.0 / (n - 1)

    u_ref = np.zeros((n, n), dtype=np.float64)
    f_ref = np.zeros((n, n), dtype=np.float64)
    f_ref[1:-1, 1:-1] = 1.0
    u_expected = vcycle_numpy(u_ref, f_ref, h, n_pre, n_post, coarse_sweeps)

    u_backend = cnp.zeros((n, n), dtype=cnp.float64)
    f_backend = cnp.zeros((n, n), dtype=cnp.float64)
    u_backend[:, :] = 0.0
    f_backend[:, :] = 0.0
    f_backend[1:-1, 1:-1] = 1.0
    vcycle(u_backend, f_backend, h, n_pre, n_post, coarse_sweeps)
    u_actual = u_backend.get(interface)

    print("")
    return summarize_diff(
        "One V-cycle validation against NumPy",
        u_actual,
        u_expected,
    )


def validate_vcycles(interface, n, cycles, n_pre, n_post, coarse_sweeps):
    """Compare multiple backend V-cycles in one flushed DAG against NumPy."""
    h = 1.0 / (n - 1)

    u_ref = np.zeros((n, n), dtype=np.float64)
    f_ref = np.zeros((n, n), dtype=np.float64)
    f_ref[1:-1, 1:-1] = 1.0
    u_expected = vcycles_numpy(u_ref, f_ref, h, cycles, n_pre, n_post, coarse_sweeps)

    u_backend = cnp.zeros((n, n), dtype=cnp.float64)
    f_backend = cnp.zeros((n, n), dtype=cnp.float64)
    u_backend[:, :] = 0.0
    f_backend[:, :] = 0.0
    f_backend[1:-1, 1:-1] = 1.0
    for _ in range(cycles):
        vcycle(u_backend, f_backend, h, n_pre, n_post, coarse_sweeps)
    u_actual = u_backend.get(interface)

    print("")
    return summarize_diff(
        f"{cycles} batched V-cycle{'s' if cycles != 1 else ''} validation against NumPy",
        u_actual,
        u_expected,
    )


def validate_vcycle_stages(interface, n, n_pre, n_post, coarse_sweeps):
    """Compare top-level intermediate arrays from one backend V-cycle."""
    h = 1.0 / (n - 1)

    u_ref = np.zeros((n, n), dtype=np.float64)
    f_ref = np.zeros((n, n), dtype=np.float64)
    f_ref[1:-1, 1:-1] = 1.0
    expected = vcycle_numpy_capture(u_ref, f_ref, h, n_pre, n_post, coarse_sweeps)

    u_backend = cnp.zeros((n, n), dtype=cnp.float64)
    f_backend = cnp.zeros((n, n), dtype=cnp.float64)
    u_backend[:, :] = 0.0
    f_backend[:, :] = 0.0
    f_backend[1:-1, 1:-1] = 1.0
    actual_arrays = vcycle_capture_backend(u_backend, f_backend, h, n_pre, n_post, coarse_sweeps)

    stage_order = ("u_pre", "r", "r_coarse", "e_coarse", "u_corr", "u_final")
    execute(interface)
    print("")
    worst_max = 0.0
    worst_l2 = 0.0
    for stage in stage_order:
        backend_arr = actual_arrays[stage]
        reference = expected[stage]
        if backend_arr is None or reference is None or reference.size == 0:
            continue
        backend = backend_arr.get(interface)
        max_abs, l2_err = summarize_diff(
            f"V-cycle stage `{stage}` against NumPy",
            backend,
            reference,
        )
        print("")
        worst_max = max(worst_max, max_abs)
        worst_l2 = max(worst_l2, l2_err)
    return worst_max, worst_l2


def parse_args():
    parser = argparse.ArgumentParser(description="Run or validate the 2D multigrid example.")
    parser.add_argument("--mode", choices=(
                        "solve",
                        "validate-restrict",
                        "validate-prolong",
                        "validate-residual",
                        "validate-jacobi",
                        "validate-vcycle",
                        "validate-vcycles",
                        "validate-vcycle-stages",
                        "validate-all",
                        "both"),
                        default="solve",
                        help="Run the full V-cycle solve or targeted NumPy validation checks.")
    parser.add_argument("--host", default="192.168.1.115",
                        help="Charm++ server host")
    parser.add_argument("--port", type=int, default=1234,
                        help="Charm++ server port")
    parser.add_argument("--odf", type=int, default=4,
                        help="Object decomposition factor")
    parser.add_argument("--n", type=int, default=129,
                        help="Grid size (must be 2^k + 1)")
    parser.add_argument("--pre", type=int, default=5,
                        help="Number of pre-smoothing sweeps")
    parser.add_argument("--post", type=int, default=5,
                        help="Number of post-smoothing sweeps")
    parser.add_argument("--coarse-sweeps", type=int, default=400,
                        help="Jacobi sweeps on the coarsest grid")
    parser.add_argument("--cycles", type=int, default=10,
                        help="Number of V-cycles to run")
    parser.add_argument("--check-every", type=int, default=2,
                        help="Fetch the solution and print a residual every N cycles")
    return parser.parse_args()


def main():
    args = parse_args()
    # Grid parameters: n x n grid on [0,1]^2
    # n must be 2^k + 1 for multigrid coarsening to work cleanly
    n = args.n
    h = 1.0 / (n - 1)

    # Solution array with zero initial guess
    u = cnp.zeros((n, n), dtype=cnp.float64)

    # Boundary conditions: u = 0 on all boundaries (already set)
    # Could set non-trivial BCs here, e.g.:
    # u[0, :] = 1.0  # top boundary = 1

    # Right-hand side: f = 2*pi^2 * sin(pi*x) * sin(pi*y)
    # (exact solution is u = sin(pi*x) * sin(pi*y))
    f = cnp.zeros((n, n), dtype=cnp.float64)

    # Since we can't fill f point-by-point efficiently in this DSL,
    # we set a uniform RHS for demonstration.
    # f = 1.0 gives Poisson equation -Lap(u) = 1 with u=0 on boundary.
    f[1:-1, 1:-1] = 1.0

    # Connect to backend
    interface = cnp.CharmNumericInterface()
    interface.connect(args.host, args.port, args.odf)
    set_auto_flush(interface, 1000)

    if args.mode in ("validate-restrict", "both"):
        validate_restriction(interface, n)
        if args.mode == "validate-restrict":
            return

    if args.mode in ("validate-prolong", "validate-all"):
        validate_prolongation(interface, n)
        if args.mode == "validate-prolong":
            return

    if args.mode in ("validate-residual", "validate-all"):
        validate_residual(interface, n)
        if args.mode == "validate-residual":
            return

    if args.mode in ("validate-jacobi", "validate-all"):
        validate_jacobi(interface, n, n_smooth=args.pre)
        if args.mode == "validate-jacobi":
            return

    if args.mode in ("validate-vcycle", "validate-all"):
        validate_vcycle(interface, n, args.pre, args.post, args.coarse_sweeps)
        if args.mode == "validate-vcycle":
            return

    if args.mode in ("validate-vcycles", "validate-all"):
        validate_vcycles(interface, n, args.cycles, args.pre, args.post, args.coarse_sweeps)
        if args.mode == "validate-vcycles":
            return

    if args.mode in ("validate-vcycle-stages", "validate-all"):
        validate_vcycle_stages(interface, n, args.pre, args.post, args.coarse_sweeps)
        if args.mode == "validate-vcycle-stages":
            return

    if args.mode == "validate-all":
        return

    # V-cycle iterations
    # Slightly stronger smoothing tends to make the residual trend more monotone.
    n_pre = args.pre
    n_post = args.post
    coarse_sweeps = args.coarse_sweeps
    n_cycles = args.cycles
    check_every = max(1, args.check_every)

    for cycle in range(n_cycles):
        vcycle(u, f, h, n_pre, n_post, coarse_sweeps)

        # Execute and check solution periodically
        if (cycle + 1) % check_every == 0:
            result = u.get(interface)
            # Compute residual norm using numpy on retrieved data
            r = np.zeros_like(result)
            r[1:-1, 1:-1] = result[:-2, 1:-1] + result[2:, 1:-1] + \
                            result[1:-1, :-2] + result[1:-1, 2:] - \
                            4 * result[1:-1, 1:-1]
            r[1:-1, 1:-1] = 1.0 - (-r[1:-1, 1:-1] / (h * h))  # f - (-Lap u)
            res_norm = np.linalg.norm(r)
            print(f"V-cycle {cycle + 1}: residual norm = {res_norm:.6e}")

    # Final result
    execute(interface)
    result = u.get(interface)
    print(f"\nFinal solution: min = {result.min():.6f}, max = {result.max():.6f}")
    print(f"Grid size: {n} x {n}, h = {h:.6f}")


if __name__ == '__main__':
    main()
